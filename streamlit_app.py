import json
import re
from typing import Any, Generator, Iterator

import pandas as pd
import requests
import snowflake.connector
import sseclient
import streamlit as st
import pyotp  # For TOTP (if you have the secret)

# Set app title
st.set_page_config(page_title="Snowflake Login", page_icon="❄️")

st.title("🔐 Login to Snowflake")

# Session state init
if "conn" not in st.session_state:
    st.session_state.conn = None
if "login_failed" not in st.session_state:
    st.session_state.login_failed = False

# Login form
with st.form("login_form"):
    username = st.text_input("Username", value="", placeholder="e.g. john.doe")
    password = st.text_input("Password", type="password")
    mfa_code = st.text_input("MFA Code (TOTP)", placeholder="e.g. 123456")
    submitted = st.form_submit_button("Login")

    if submitted:
        try:
            # Combine password + MFA code if provided
            full_password = password + mfa_code if mfa_code else password

            conn = snowflake.connector.connect(
                user=username,
                password=full_password,
                account="NIBRWBR-MANA",  # Or from st.secrets or text_input
                warehouse="COMPUTE_WH",
                database="SALESFORCE_DB",
                schema="SALESFORCE",
                role="SALESFORCEDB"
            )
            st.session_state.conn = conn
            st.success("✅ Connected to Snowflake successfully!")
            st.session_state.login_failed = False

        except Exception as e:
            st.session_state.login_failed = True
            st.error(f"❌ Failed to connect: {e}")

# Once logged in, show rest of app
if st.session_state.conn:
    st.write("🎉 You're now connected to Snowflake!")
    # Place the rest of your analyst app logic here



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
