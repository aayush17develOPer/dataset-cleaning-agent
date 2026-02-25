"""
app.py — Streamlit UI for the Data Cleaning Agent
Run: streamlit run app.py
"""

import os
import sys
import tempfile
import streamlit as st
import pandas as pd

# ── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="DataClean AI",
    page_icon="🧹",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

  html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

  /* Dark gradient background */
  .stApp {
    background: linear-gradient(135deg, #0f0c29, #302b63, #24243e);
    color: #e0e0f0;
  }

  /* Hero header */
  .hero {
    text-align: center;
    padding: 2.5rem 1rem 1.5rem;
  }
  .hero h1 {
    font-size: 3rem;
    font-weight: 700;
    background: linear-gradient(90deg, #a78bfa, #60a5fa, #34d399);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    margin-bottom: 0.3rem;
  }
  .hero p {
    color: #9ca3af;
    font-size: 1.1rem;
    margin-top: 0;
  }

  /* Upload box */
  [data-testid="stFileUploader"] {
    border: 2px dashed #4f46e5 !important;
    border-radius: 16px !important;
    background: rgba(79, 70, 229, 0.07) !important;
    padding: 1.5rem !important;
    transition: border-color 0.2s;
  }
  [data-testid="stFileUploader"]:hover {
    border-color: #818cf8 !important;
  }

  /* Run button */
  div.stButton > button {
    width: 100%;
    background: linear-gradient(90deg, #4f46e5, #7c3aed);
    color: white;
    border: none;
    border-radius: 12px;
    padding: 0.8rem 2rem;
    font-size: 1rem;
    font-weight: 600;
    cursor: pointer;
    transition: opacity 0.2s, transform 0.1s;
    box-shadow: 0 4px 20px rgba(79, 70, 229, 0.4);
  }
  div.stButton > button:hover {
    opacity: 0.9;
    transform: translateY(-1px);
  }

  /* Result cards */
  .result-card {
    background: rgba(255,255,255,0.05);
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 16px;
    padding: 1.5rem;
    margin-bottom: 1rem;
    backdrop-filter: blur(10px);
  }
  .result-card h3 {
    font-size: 1rem;
    font-weight: 600;
    margin-bottom: 0.8rem;
  }

  /* Step pills */
  .step-pill {
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    background: rgba(255,255,255,0.07);
    border-radius: 999px;
    padding: 0.3rem 0.9rem;
    margin: 0.2rem 0.2rem;
    font-size: 0.85rem;
    border: 1px solid rgba(255,255,255,0.12);
  }
  .step-pill.done  { border-color: #34d399; color: #34d399; }
  .step-pill.error { border-color: #f87171; color: #f87171; }
  .step-pill.retry { border-color: #fbbf24; color: #fbbf24; }

  /* Metric chips */
  .metric-row { display: flex; gap: 1rem; flex-wrap: wrap; margin-bottom: 1rem; }
  .metric-chip {
    background: rgba(79,70,229,0.18);
    border: 1px solid rgba(79,70,229,0.35);
    border-radius: 10px;
    padding: 0.6rem 1.2rem;
    text-align: center;
    min-width: 120px;
  }
  .metric-chip .val { font-size: 1.4rem; font-weight: 700; color: #a78bfa; }
  .metric-chip .lbl { font-size: 0.75rem; color: #9ca3af; margin-top: 0.1rem; }

  /* Download button override */
  [data-testid="stDownloadButton"] button {
    background: linear-gradient(90deg, #059669, #10b981) !important;
    color: white !important;
    border: none !important;
    border-radius: 10px !important;
    font-weight: 600 !important;
    padding: 0.6rem 1.4rem !important;
    box-shadow: 0 4px 14px rgba(16, 185, 129, 0.35) !important;
  }

  /* Tabs */
  .stTabs [data-baseweb="tab-list"] {
    background: rgba(255,255,255,0.05);
    border-radius: 12px;
    padding: 4px;
    gap: 4px;
  }
  .stTabs [data-baseweb="tab"] {
    border-radius: 8px;
    color: #9ca3af;
    font-weight: 500;
  }
  .stTabs [aria-selected="true"] {
    background: rgba(79,70,229,0.35) !important;
    color: #e0e0f0 !important;
  }

  /* Text areas / code */
  .stTextArea textarea {
    background: rgba(0,0,0,0.3) !important;
    color: #d1fae5 !important;
    border-radius: 10px !important;
    border: 1px solid rgba(255,255,255,0.1) !important;
    font-family: 'Fira Code', monospace !important;
    font-size: 0.85rem !important;
  }

  /* Dataframe */
  [data-testid="stDataFrame"] { border-radius: 12px; overflow: hidden; }

  /* Status widget */
  [data-testid="stStatus"] {
    background: rgba(255,255,255,0.04) !important;
    border: 1px solid rgba(255,255,255,0.1) !important;
    border-radius: 12px !important;
  }
</style>
""", unsafe_allow_html=True)

# ── Hero ──────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="hero">
  <h1>🧹 DataClean AI</h1>
  <p>Autonomous data cleaning & feature engineering — powered by LangGraph + LLM</p>
</div>
""", unsafe_allow_html=True)

st.divider()

# ── Layout ────────────────────────────────────────────────────────────────────
left, right = st.columns([1, 2], gap="large")

with left:
    st.markdown("#### 📂 Upload your CSV")
    uploaded = st.file_uploader(
        label="Drop a CSV file here",
        type=["csv"],
        label_visibility="collapsed",
    )

    if uploaded:
        preview_df = pd.read_csv(uploaded)
        uploaded.seek(0)

        st.markdown(f"""
        <div class="metric-row">
          <div class="metric-chip">
            <div class="val">{preview_df.shape[0]}</div>
            <div class="lbl">Rows</div>
          </div>
          <div class="metric-chip">
            <div class="val">{preview_df.shape[1]}</div>
            <div class="lbl">Columns</div>
          </div>
          <div class="metric-chip">
            <div class="val">{preview_df.isnull().sum().sum()}</div>
            <div class="lbl">Missing cells</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        with st.expander("👀 Preview (first 5 rows)", expanded=False):
            st.dataframe(preview_df.head(), use_container_width=True)

    st.markdown("#### ⚙️ Settings")
    max_retries = st.slider("Max self-correction retries", 1, 5, 3)

    run_btn = st.button("🚀 Run Cleaning Agent", disabled=not uploaded)

# ── Right panel — results ─────────────────────────────────────────────────────
with right:
    if not uploaded:
        st.markdown("""
        <div class="result-card" style="text-align:center; padding: 3rem 1rem; color: #6b7280;">
          <div style="font-size: 3rem; margin-bottom: 0.8rem">🔬</div>
          <div style="font-size: 1.1rem; font-weight: 600; color: #9ca3af;">Upload a CSV to get started</div>
          <div style="margin-top: 0.5rem; font-size: 0.9rem;">The agent will inspect, clean, and suggest features automatically.</div>
        </div>
        """, unsafe_allow_html=True)

    elif run_btn:
        # ── Save uploaded file to temp ────────────────────────────────────
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f_in:
            f_in.write(uploaded.read())
            input_path = f_in.name

        out_dir = os.path.join(tempfile.gettempdir(), "dataclean_ai")
        os.makedirs(out_dir, exist_ok=True)
        output_path = os.path.join(out_dir, "cleaned_output.csv")

        # ── Import graph ──────────────────────────────────────────────────
        from agent.graph import build_graph
        from agent import nodes as agent_nodes

        agent_nodes.MAX_RETRIES = max_retries

        # ── Live progress using graph.stream() ───────────────────────────
        NODE_LABELS = {
            "inspect":       ("🔍", "Inspecting dataset"),
            "plan":          ("🧠", "Planning cleaning strategy"),
            "generate_code": ("✍️", "Generating cleaning code"),
            "execute_code":  ("⚙️", "Executing code"),
            "debug":         ("🔧", "Self-correcting code"),
            "feature_eng":   ("📊", "Planning feature engineering"),
        }

        results = {}
        progress_placeholder = st.empty()

        completed_steps = []
        cleaning_plan = ""
        fe_plan = ""
        cleaned_csv_path = None
        error_occurred = False

        with st.status("🤖 Agent is running...", expanded=True) as status_box:
            graph = build_graph()

            for event in graph.stream({
                "raw_csv_path":             input_path,
                "output_csv_path":          output_path,
                "data_profile":             {},
                "cleaning_plan":            "",
                "generated_code":           "",
                "error":                    None,
                "cleaned_csv_path":         None,
                "feature_engineering_plan": "",
            }):
                for node_name, node_state in event.items():
                    if node_name in ("__start__", "__end__"):
                        continue

                    icon, label = NODE_LABELS.get(node_name, ("▶️", node_name))

                    # Capture results from state updates
                    if "cleaning_plan" in node_state and node_state["cleaning_plan"]:
                        cleaning_plan = node_state["cleaning_plan"]
                    if "feature_engineering_plan" in node_state and node_state["feature_engineering_plan"]:
                        fe_plan = node_state["feature_engineering_plan"]
                    if "cleaned_csv_path" in node_state and node_state["cleaned_csv_path"]:
                        cleaned_csv_path = node_state["cleaned_csv_path"]
                    if "error" in node_state and node_state["error"]:
                        error_occurred = True

                    pill_class = "retry" if node_name == "debug" else ("error" if (node_name == "execute_code" and node_state.get("error")) else "done")
                    completed_steps.append((icon, label, pill_class))

                    # Render completed steps
                    pills_html = "".join(
                        f'<span class="step-pill {cls}">{ico} {lbl}</span>'
                        for ico, lbl, cls in completed_steps
                    )
                    st.markdown(pills_html, unsafe_allow_html=True)

            status_box.update(label="✅ Agent finished!", state="complete", expanded=False)

        # ── Results tabs ─────────────────────────────────────────────────
        tab1, tab2, tab3 = st.tabs(["🧠 Cleaning Plan", "📊 Feature Engineering Plan", "📁 Cleaned CSV"])

        with tab1:
            if cleaning_plan:
                st.markdown(f"""<div class="result-card">
                  <h3>🧠 Cleaning Plan</h3>
                  <p style="white-space: pre-wrap; color: #d1d5db; line-height: 1.7; font-size: 0.9rem;">{cleaning_plan}</p>
                </div>""", unsafe_allow_html=True)
            else:
                st.info("No cleaning plan captured.")

        with tab2:
            if fe_plan:
                st.markdown(f"""<div class="result-card">
                  <h3>📊 Feature Engineering Plan</h3>
                  <p style="white-space: pre-wrap; color: #d1d5db; line-height: 1.7; font-size: 0.9rem;">{fe_plan}</p>
                </div>""", unsafe_allow_html=True)
            else:
                st.info("No feature engineering plan available.")

        with tab3:
            if cleaned_csv_path and os.path.exists(cleaned_csv_path):
                cleaned_df = pd.read_csv(cleaned_csv_path)

                # Stats row
                orig_shape = preview_df.shape
                st.markdown(f"""
                <div class="metric-row">
                  <div class="metric-chip">
                    <div class="val">{cleaned_df.shape[0]}</div><div class="lbl">Rows</div>
                  </div>
                  <div class="metric-chip">
                    <div class="val">{cleaned_df.shape[1]}</div><div class="lbl">Columns</div>
                  </div>
                  <div class="metric-chip">
                    <div class="val">{cleaned_df.isnull().sum().sum()}</div><div class="lbl">Missing cells</div>
                  </div>
                  <div class="metric-chip">
                    <div class="val" style="color:#34d399">{orig_shape[0] - cleaned_df.shape[0]:+}</div>
                    <div class="lbl">Rows removed</div>
                  </div>
                </div>
                """, unsafe_allow_html=True)

                st.dataframe(cleaned_df, use_container_width=True)

                st.download_button(
                    label="⬇️ Download Cleaned CSV",
                    data=cleaned_df.to_csv(index=False).encode("utf-8"),
                    file_name="cleaned_output.csv",
                    mime="text/csv",
                )
            else:
                st.error("Cleaned CSV not found — the agent may have exhausted all retries.")
