"""
main.py — Entry point for the Data Cleaning Agent

Usage:
    python main.py                          # uses default dirty test CSV
    python main.py data/raw/custom.csv      # pass your own CSV
"""

import sys
import os
from agent.graph import build_graph

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
INPUT_CSV  = sys.argv[1] if len(sys.argv) > 1 else os.path.join(BASE_DIR, "data", "raw", "dirty_titanic.csv")
OUTPUT_CSV = os.path.join(BASE_DIR, "data", "cleaned", "cleaned_output.csv")

os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

# ── Run ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"\n🚀 Starting Data Cleaning Agent")
    print(f"   Input  : {INPUT_CSV}")
    print(f"   Output : {OUTPUT_CSV}\n")

    graph = build_graph()

    final_state = graph.invoke({
        "raw_csv_path":           INPUT_CSV,
        "output_csv_path":        OUTPUT_CSV,
        "data_profile":           {},
        "cleaning_plan":          "",
        "generated_code":         "",
        "error":                  None,
        "cleaned_csv_path":       None,
        "feature_engineering_plan": "",
    })

    print("\n" + "═" * 60)
    print("✅ Pipeline Complete!")
    print("═" * 60)
    print(f"\n📁 Cleaned CSV saved to:\n   {final_state.get('cleaned_csv_path', 'N/A')}")
    print(f"\n📊 Feature Engineering Plan:\n")
    print(final_state.get("feature_engineering_plan", "N/A"))
