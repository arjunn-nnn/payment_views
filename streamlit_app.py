import json
import re
from typing import Any, Generator, Iterator

import pandas as pd
import requests
import snowflake.connector
import sseclient
import streamlit as st


def login_screen():
    st.title("🔐 Login to Snowflake")

    with st.form("login_form", clear_on_submit=False):
        user = st.text_input("Username", key="username")
        password = st.text_input("Password", type="password", key="password")
        mfa_code = st.text_input("MFA Code (TOTP)", key="mfa")
        submitted = st.form_submit_button("Login")

    if submitted:
        if not user or not password:
            st.error("Username and password required.")
            return False

        # Combine password + TOTP if MFA is provided
        full_password = password + mfa_code.strip() if mfa_code else password

        try:
            conn = snowflake.connector.connect(
                user=user,
                password=full_password,
                account="NIBRWBR-MANA",
                warehouse="COMPUTE_WH",
                role="SALESFORCEDB",
                database="SALESFORCE_DB",
                schema="SALESFORCE"
            )
            st.session_state.conn = conn
            st.session_state.logged_in = True
            st.rerun()
        except Exception as e:
            st.error(f"❌ Failed to connect to Snowflake: {e}")
            return False
    return False


# ------------- Cortex Analyst Logic ------------- #

def get_conversation_history() -> list[dict[str, Any]]:
    messages = []
    for msg in st.session_state.messages:
        m = {"role": msg["role"], "content": []}
        for content in msg["content"]:
            if isinstance(content, pd.DataFrame) or isinstance(content, Exception):
                continue
            else:
                m["content"].append({"type": "text", "text": content})
        messages.append(m)
    return messages


def send_message() -> requests.Response:
    request_body = {
        "messages": get_conversation_history(),
        "semantic_model_file": "@SALESFORCE_DB.SALESFORCE.PAYMENTS/payment_model.smd",  # Update as needed
        "stream": True,
    }
    resp = requests.post(
        url=f"https://{st.session_state.conn.host}/api/v2/cortex/analyst/message",
        json=request_body,
        headers={"Content-Type": "application/json"},
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
        new_block = event.event != "message.content.delta" or data["index"] != prev_index

        if prev_type == "sql" and new_block:
            yield "\n```\n\n"
        if event.event == "message.content.delta":
            if data["type"] == "sql":
                if new_block:
                    yield "```sql\n"
                yield data["statement_delta"]
            elif data["type"] == "text":
                yield data["text_delta"]
            elif data["type"] == "suggestions":
                if new_block:
                    yield "\nHere are some example questions:\n\n- "
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
                st.error(f"Error: {st.session_state.error}", icon="🚨")
                accumulated_content.append(Exception(st.session_state.error))
                st.session_state.error = None
                st.session_state.status = "Interpreting question"
                st.session_state.messages.pop()
                return

            sql_blocks = re.findall(r"```sql\s*(.*?)\s*```", written_content, re.DOTALL | re.IGNORECASE)
            for sql in sql_blocks:
                with st.spinner("Executing SQL..."):
                    df = pd.read_sql(sql, st.session_state.conn)
                    accumulated_content.append(df)
                    display_df(df)

    st.session_state.status = "Interpreting question"
    st.session_state.messages.append({"role": "analyst", "content": accumulated_content})


def show_history() -> None:
    for msg in st.session_state.messages:
        role = "assistant" if msg["role"] == "analyst" else "user"
        with st.chat_message(role):
            for content in msg["content"]:
                if isinstance(content, pd.DataFrame):
                    display_df(content)
                elif isinstance(content, Exception):
                    st.error(str(content), icon="🚨")
                else:
                    st.write(content)


# ----------- Streamlit UI Logic ----------- #

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if not st.session_state.logged_in:
    login_screen()
else:
    st.set_page_config(page_title="Cortex Analyst - Salesforce Payments", layout="wide")
    st.title("💬 Cortex Analyst")
    st.markdown("Semantic Model: `PAYMENTS/payment_model.smd`")

    if "messages" not in st.session_state:
        st.session_state.messages = []
        st.session_state.status = "Interpreting question"
        st.session_state.error = None

    show_history()

    if user_input := st.chat_input("What is your question?"):
        process_message(user_input)
