import json
import re
from typing import Any, Generator, Iterator

import pandas as pd
import requests
import snowflake.connector
import sseclient
import streamlit as st
import pyotp  # For TOTP (if you have the secret)

# Load Snowflake credentials
SNOWFLAKE_USER = st.secrets["snowflake"]["user"]
SNOWFLAKE_ACCOUNT = st.secrets["snowflake"]["account"]
SNOWFLAKE_WAREHOUSE = st.secrets["snowflake"]["warehouse"]
SNOWFLAKE_ROLE = st.secrets["snowflake"]["role"]
DATABASE = st.secrets["snowflake"]["database"]
SCHEMA = st.secrets["snowflake"]["schema"]
STAGE = st.secrets["snowflake"]["stage"]
FILE = st.secrets["snowflake"]["file"]
PASSWORD = st.secrets["snowflake"]["password"]
TOTP_SECRET = st.secrets["snowflake"].get("totp_secret")  # Optional

# Step 1: Get or generate the MFA code
if TOTP_SECRET:
    totp = pyotp.TOTP(TOTP_SECRET)
    current_totp = totp.now()
else:
    current_totp = st.text_input("🔐 Enter your current MFA code (TOTP):", type="password")

# Step 2: Combine password + MFA code
password_with_mfa = PASSWORD + current_totp

# Step 3: Connect to Snowflake
if "conn" not in st.session_state and current_totp:
    try:
        st.session_state.conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=password_with_mfa,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            role=SNOWFLAKE_ROLE,
            database=DATABASE,
            schema=SCHEMA,
        )
        st.success("✅ Connected to Snowflake successfully!")
    except Exception as e:
        st.error(f"❌ Failed to connect to Snowflake: {e}")



def get_conversation_history() -> list[dict[str, Any]]:
    messages = []
    for msg in st.session_state.messages:
        m = {"role": msg["role"], "content": []}
        for content in msg["content"]:
            if isinstance(content, pd.DataFrame):
                continue  # Skip dataframes for API
            elif isinstance(content, Exception):
                continue
            else:
                m["content"].append({"type": "text", "text": content})
        messages.append(m)
    return messages


def send_message() -> requests.Response:
    request_body = {
        "messages": get_conversation_history(),
        "semantic_model_file": f"@{DATABASE}.{SCHEMA}.{STAGE}/{FILE}",
        "stream": True,
    }
    # Note: No token header here since SSO session is used
    resp = requests.post(
        url=f"https://{st.session_state.conn.host}/api/v2/cortex/analyst/message",
        json=request_body,
        headers={
            "Content-Type": "application/json",
        },
        stream=True,
    )
    if resp.status_code < 400:
        return resp
    else:
        raise Exception(f"Failed request with status {resp.status_code}: {resp.text}")


def stream_events(events: Iterator[sseclient.Event]) -> Generator[Any, Any, Any]:
    prev_index = -1
    prev_type = ""
    prev_suggestion_index = -1
    while True:
        event = next(events, None)
        if not event:
            return
        data = json.loads(event.data)
        new_content_block = event.event != "message.content.delta" or data["index"] != prev_index

        if prev_type == "sql" and new_content_block:
            yield "\n```\n\n"
        if event.event == "message.content.delta":
            if data["type"] == "sql":
                if new_content_block:
                    yield "```sql\n"
                yield data["statement_delta"]
            elif data["type"] == "text":
                yield data["text_delta"]
            elif data["type"] == "suggestions":
                if new_content_block:
                    yield "\nHere are some example questions you could ask:\n\n"
                    yield "\n- "
                elif prev_suggestion_index != data["suggestions_delta"]["index"]:
                    yield "\n- "
                yield data["suggestions_delta"]["suggestion_delta"]
                prev_suggestion_index = data["suggestions_delta"]["index"]
            prev_index = data["index"]
            prev_type = data["type"]
        elif event.event == "status":
            st.session_state.status = data["status_message"]
            return
        elif event.event == "error":
            st.session_state.error = data
            return


def display_df(df: pd.DataFrame) -> None:
    if len(df.index) > 1:
        data_tab, line_tab, bar_tab = st.tabs(["Data", "Line Chart", "Bar Chart"])
        data_tab.dataframe(df)
        if len(df.columns) > 1:
            df = df.set_index(df.columns[0])
        with line_tab:
            st.line_chart(df)
        with bar_tab:
            st.bar_chart(df)
    else:
        st.dataframe(df)


def process_message(prompt: str) -> None:
    st.session_state.messages.append({"role": "user", "content": [prompt]})
    with st.chat_message("user"):
        st.markdown(prompt)

    accumulated_content = []
    with st.chat_message("assistant"):
        with st.spinner("Sending request..."):
            response = send_message()
        st.markdown(f"```request_id: {response.headers.get('X-Snowflake-Request-Id')}```")
        events = sseclient.SSEClient(response).events()
        while st.session_state.status.lower() != "done":
            with st.spinner(st.session_state.status):
                written_content = st.write_stream(stream_events(events))
                accumulated_content.append(written_content)
            if st.session_state.error:
                st.error(f"Error while processing request:\n {st.session_state.error}", icon="🚨")
                accumulated_content.append(Exception(st.session_state.error))
                st.session_state.error = None
                st.session_state.status = "Interpreting question"
                st.session_state.messages.pop()
                return
            pattern = r"```sql\s*(.*?)\s*```"
            sql_blocks = re.findall(pattern, written_content, re.DOTALL | re.IGNORECASE)
            if sql_blocks:
                for sql_query in sql_blocks:
                    with st.spinner("Executing SQL query..."):
                        df = pd.read_sql(sql_query, st.session_state.conn)
                        accumulated_content.append(df)
                        display_df(df)
    st.session_state.status = "Interpreting question"
    st.session_state.messages.append({"role": "analyst", "content": accumulated_content})


def show_conversation_history() -> None:
    for message in st.session_state.messages:
        chat_role = "assistant" if message["role"] == "analyst" else "user"
        with st.chat_message(chat_role):
            for content in message["content"]:
                if isinstance(content, pd.DataFrame):
                    display_df(content)
                elif isinstance(content, Exception):
                    st.error(f"Error while processing request:\n {content}", icon="🚨")
                else:
                    st.write(content)


# --- Streamlit UI setup ---

st.set_page_config(page_title="Cortex Analyst - Salesforce Payments", page_icon="💬", layout="wide")

st.title("💬 Cortex Analyst")
st.markdown(f"Semantic Model: `{FILE}`")

if "messages" not in st.session_state:
    st.session_state.messages = []
    st.session_state.status = "Interpreting question"
    st.session_state.error = None

show_conversation_history()

if user_input := st.chat_input("What is your question?"):
    process_message(user_input)
