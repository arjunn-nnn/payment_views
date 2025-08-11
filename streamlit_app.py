import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

import _snowflake
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.exceptions import SnowparkSQLException

# Page Config
st.set_page_config(
    page_title="Cortex Analyst - Salesforce Payments",
    page_icon="💬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Constants
AVAILABLE_SEMANTIC_MODELS_PATHS = [
    "SALESFORCE_DB.SALESFORCE.SALESFORCE/payment_views.yaml"
]
API_ENDPOINT = "/api/v2/cortex/analyst/message"
FEEDBACK_API_ENDPOINT = "/api/v2/cortex/analyst/feedback"
API_TIMEOUT = 50000  # ms

# Session
session = get_active_session()


def main():
    if "messages" not in st.session_state:
        reset_session_state()

    show_header_and_sidebar()

    with st.container():
        st.subheader("🔎 Chat with Your Data")

        if len(st.session_state.messages) == 0:
            with st.expander("💡 Try Asking", expanded=True):
                st.markdown("""
                - What was the total revenue last month?
                - How many payments failed in Q2?
                - Show payment trends by donor type.
                """)
            process_user_input("What questions can I ask?")
        
        display_conversation()
        handle_user_inputs()
        handle_error_notifications()
        display_warnings()

    show_footer()


def reset_session_state():
    st.session_state.messages = []
    st.session_state.active_suggestion = None
    st.session_state.warnings = []
    st.session_state.form_submitted = {}


def show_header_and_sidebar():
    st.title("💬 Cortex Analyst")
    st.markdown("""
        <style>
        .intro-text {
            font-size: 1.1rem;
            color: #4B4B4B;
        }
        </style>
        <div class='intro-text'>
            This chatbot provides human-friendly, natural language responses for querying and exploring <b>payment data</b> from the Salesforce table.
        </div>
    """, unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("### 🧠 Semantic Model")
        st.info("Choose the semantic YAML model to guide natural language interpretation.")
        st.selectbox(
            "Choose a model:",
            AVAILABLE_SEMANTIC_MODELS_PATHS,
            format_func=lambda s: s.split("/")[-1],
            key="selected_semantic_model_path",
            on_change=reset_session_state,
        )
        st.divider()
        if st.button("🧹 Clear Chat History", use_container_width=True):
            reset_session_state()


def handle_user_inputs():
    user_input = st.chat_input("What is your question?")
    if user_input:
        process_user_input(user_input)
    elif st.session_state.active_suggestion is not None:
        suggestion = st.session_state.active_suggestion
        st.session_state.active_suggestion = None
        process_user_input(suggestion)


def handle_error_notifications():
    if st.session_state.get("fire_API_error_notify"):
        st.toast("An API error has occurred!", icon="🚨")
        st.session_state["fire_API_error_notify"] = False


def process_user_input(prompt: str):
    st.session_state.warnings = []

    new_user_message = {
        "role": "user",
        "content": [{"type": "text", "text": prompt}],
    }
    st.session_state.messages.append(new_user_message)
    with st.chat_message("user"):
        user_msg_index = len(st.session_state.messages) - 1
        display_message(new_user_message["content"], user_msg_index)

    with st.chat_message("analyst"):
        with st.spinner("Waiting for Analyst's response..."):
            time.sleep(1)
            response, error_msg = get_analyst_response(st.session_state.messages)
            if error_msg is None:
                analyst_message = {
                    "role": "analyst",
                    "content": response["message"]["content"],
                    "request_id": response["request_id"],
                }
            else:
                analyst_message = {
                    "role": "analyst",
                    "content": [{"type": "text", "text": error_msg}],
                    "request_id": response.get("request_id"),
                }
                st.session_state["fire_API_error_notify"] = True

            if "warnings" in response:
                st.session_state.warnings = response["warnings"]

            st.session_state.messages.append(analyst_message)
            st.rerun()


def get_analyst_response(messages: List[Dict]) -> Tuple[Dict, Optional[str]]:
    request_body = {
        "messages": messages,
        "semantic_model_file": f"@{st.session_state.selected_semantic_model_path}",
    }

    resp = _snowflake.send_snow_api_request(
        "POST", API_ENDPOINT, {}, {}, request_body, None, API_TIMEOUT
    )

    parsed_content = json.loads(resp["content"])
    if resp["status"] < 400:
        return parsed_content, None
    else:
        error_msg = (
            f"🚨 An Analyst API error has occurred 🚨\n\n"
            f"* response code: `{resp['status']}`\n"
            f"* request-id: `{parsed_content.get('request_id', 'N/A')}`\n"
            f"* error code: `{parsed_content.get('error_code', 'N/A')}`\n\n"
            f"Message:\n```\n{parsed_content.get('message', 'No message')}\n```"
        )
        return parsed_content, error_msg


def display_conversation():
    for idx, message in enumerate(st.session_state.messages):
        role = message["role"]
        content = message["content"]
        with st.chat_message(role):
            if role == "analyst":
                display_message(content, idx, message.get("request_id"))
            else:
                display_message(content, idx)


def display_message(content: List[Dict[str, Union[str, Dict]]], message_index: int, request_id: Union[str, None] = None):
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            for suggestion_index, suggestion in enumerate(item["suggestions"]):
                if st.button(suggestion, key=f"suggestion_{message_index}_{suggestion_index}"):
                    st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            display_sql_query(item["statement"], message_index, item.get("confidence"), request_id)


@st.cache_data(show_spinner=False)
def get_query_exec_result(query: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    global session
    try:
        df = session.sql(query).to_pandas()
        return df, None
    except SnowparkSQLException as e:
        return None, str(e)


def display_sql_query(sql: str, message_index: int, confidence: dict, request_id: Union[str, None] = None):
    with st.expander("🧾 SQL Query", expanded=False):
        st.code(sql, language="sql")
        display_sql_confidence(confidence)

    with st.expander("📊 Query Results", expanded=True):
        with st.spinner("Running SQL..."):
            df, err_msg = get_query_exec_result(sql)
            if df is None:
                st.error(f"Could not execute generated SQL query. Error: {err_msg}")
            elif df.empty:
                st.info("Query returned no data.")
            else:
                data_tab, chart_tab = st.tabs(["📄 Data Table", "📈 Visualize"])
                with data_tab:
                    st.dataframe(df, use_container_width=True)
                with chart_tab:
                    display_charts_tab(df, message_index)

    if request_id:
        display_feedback_section(request_id)


def display_charts_tab(df: pd.DataFrame, message_index: int) -> None:
    if len(df.columns) >= 2:
        all_cols = list(df.columns)
        col1, col2 = st.columns(2)
        x_col = col1.selectbox("X axis", all_cols, key=f"x_col_{message_index}")
        y_col = col2.selectbox("Y axis", [c for c in all_cols if c != x_col], key=f"y_col_{message_index}")
        chart_type = st.selectbox("Chart Type", ["Line Chart 📈", "Bar Chart 📊"], key=f"chart_type_{message_index}")
        if chart_type == "Line Chart 📈":
            st.line_chart(df.set_index(x_col)[y_col])
        else:
            st.bar_chart(df.set_index(x_col)[y_col])
    else:
        st.info("At least two columns are needed to display a chart.")


def display_sql_confidence(confidence: dict):
    if not confidence or not confidence.get("verified_query_used"):
        return
    vq = confidence["verified_query_used"]
    with st.popover("📘 Verified Query Used"):
        st.text(f"Name: {vq['name']}")
        st.text(f"Question: {vq['question']}")
        st.text(f"Verified by: {vq['verified_by']}")
        st.text(f"Verified at: {datetime.fromtimestamp(vq['verified_at'])}")
        st.text("SQL:")
        st.code(vq['sql'], language="sql", wrap_lines=True)


def display_warnings():
    for warning in st.session_state.warnings:
        msg = warning.get("message", "")
        # Skip known "duplicate synonym" warnings
        if "synonyms are duplicated in the same table" in msg:
            continue
        st.warning(msg, icon="⚠️")


def display_feedback_section(request_id: str):
    with st.expander("📝 Give Feedback"):
        feedback = st.text_area("Let us know if the answer was helpful or needs improvement.")
        if st.button("Submit Feedback", key=f"feedback_{request_id}"):
            feedback_data = {
                "request_id": request_id,
                "feedback": feedback,
            }
            _snowflake.send_snow_api_request(
                "POST", FEEDBACK_API_ENDPOINT, {}, {}, feedback_data, None, API_TIMEOUT
            )
            st.success("Thank you! Feedback submitted.")


def show_footer():
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: gray;'>"
        "Made with ❤️ using Streamlit and Snowflake Cortex Analyst"
        "</div>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
