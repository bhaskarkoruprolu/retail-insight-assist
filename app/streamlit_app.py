import sys
from pathlib import Path

# Add project root to PYTHONPATH
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

import streamlit as st
from orchestration.graph import build_graph


# -----------------------------
# App setup
# -----------------------------

st.set_page_config(
    page_title="Retail Insights Assistant",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Retail Insights Assistant")
st.caption("Ask business questions and get validated insights from your data.")

# -----------------------------
# Initialize system
# -----------------------------

@st.cache_resource
def load_graph():
    return build_graph()

app = load_graph()

# Session state for chat history
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# -----------------------------
# User input
# -----------------------------

user_query = st.text_input(
    "Ask a business question",
    placeholder="e.g. Which category generated the highest revenue last quarter?"
)

submit = st.button("Run")

# -----------------------------
# Run query
# -----------------------------

if submit and user_query:

    with st.spinner("Analyzing..."):
        result = app.invoke({"user_query": user_query})

    # Save conversation
    st.session_state.chat_history.append({
        "question": user_query,
        "result": result
    })

# -----------------------------
# Display chat history
# -----------------------------

for turn in reversed(st.session_state.chat_history):

    st.markdown("### 🧑‍💼 Question")
    st.write(turn["question"])

    result = turn["result"]
    
    # Out-of-scope handling
    if result.get("status") == "blocked":
        st.error(
            "❌ This question is outside the supported analytics domain.\n\n"
            "I can answer questions related to retail sales, revenue, trends, and performance."
        )
        st.divider()
        continue

    # Validation status
    validation = result.get("validation", {})
    status = validation.get("status", "pass")

    if status == "warn":
        st.warning("⚠️ Data Quality Warning")
        for issue in validation.get("issues", []):
            st.write(f"- {issue}")

    elif status == "block":
        st.error("❌ Query Blocked")
        for issue in validation.get("issues", []):
            st.write(f"- {issue}")
        continue

    # Insight
    insight = result.get("insight", {}).get("insight", "")
    if insight:
        st.markdown("### 📈 Insight")
        st.write(insight)

    # Optional: show data
    with st.expander("🔍 View underlying data"):
        df = result.get("data_output", {}).get("result")
        if df is not None:
            st.dataframe(df)

    st.divider()
