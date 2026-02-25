"""
graph.py — LangGraph wiring for the Data Cleaning Agent

Flow:
  inspect → plan → generate_code → execute_code
                                        │
                        ┌───────────────┴───────────────┐
                        │ error                          │ success
                        ▼                               ▼
                    debug_code                feature_engineering
                        │                               │
                        └──────► execute_code           END
                                  (up to 3 retries)

NOTE: LangGraph requires a single shared TypedDict as graph state.
      We define `GraphState` here and adapt each node to read/write from it.
"""

import os
from typing import TypedDict, Optional
from langgraph.graph import StateGraph, END

from .nodes import (
    inspect_dataset,
    plan_cleaning,
    generate_code,
    execute_code,
    debug_code, 
    feature_engineering_plan,
    reset_retries,
    retry_count,
    MAX_RETRIES,
)
from .state import Input, Inspect, Plan, GenerateCode, ExecuteCode


# ── Shared Graph State ────────────────────────────────────────────────────────
# LangGraph passes this dict between every node. Each node reads what it needs
# and returns a partial dict with only the keys it updates.

class GraphState(TypedDict):
    # Input
    raw_csv_path: str
    output_csv_path: str

    # Inspection
    data_profile: dict

    # Planning
    cleaning_plan: str

    # Code generation & execution
    generated_code: str
    error: Optional[str]
    cleaned_csv_path: Optional[str]

    # Feature engineering
    feature_engineering_plan: str



# ── Node Adapters ─────────────────────────────────────────────────────────────
# Each adapter bridges GraphState ↔ the typed Pydantic state per node.

def node_inspect(state: GraphState) -> dict:
    result = inspect_dataset(Input(raw_csv_path=state["raw_csv_path"]))
    return {"data_profile": result.data_profile}


def node_plan(state: GraphState) -> dict:
    result = plan_cleaning(Inspect(data_profile=state["data_profile"]))
    return {"cleaning_plan": result.cleaning_plan}


def node_generate_code(state: GraphState) -> dict:
    result = generate_code(Plan(cleaning_plan=state["cleaning_plan"]))
    return {"generated_code": result.generated_code}


def node_execute_code(state: GraphState) -> dict:
    result = execute_code(
        GenerateCode(generated_code=state["generated_code"]),
        input_csv_path=state["raw_csv_path"],
        output_csv_path=state["output_csv_path"],
    )
    return {"error": result.error, "cleaned_csv_path": result.cleaned_csv_path}


def node_debug(state: GraphState) -> dict:
    result = debug_code(
        ExecuteCode(error=state["error"], cleaned_csv_path=state["cleaned_csv_path"]),
        original_code=state["generated_code"],
    )
    return {"generated_code": result.generated_code}


def node_feature_engineering(state: GraphState) -> dict:
    result = feature_engineering_plan(
        ExecuteCode(cleaned_csv_path=state["cleaned_csv_path"])
    )
    return {"feature_engineering_plan": result.feature_engineering_plan}


# ── Conditional Router ────────────────────────────────────────────────────────
# Called after execute_code. Decides: retry → "debug" | success → "feature_eng" | give up → END

def route_after_execute(state: GraphState) -> str:
    from . import nodes  # import module to read current retry_count

    if state["error"] is None:
        return "feature_eng"                     # ✅ success path

    if nodes.retry_count < MAX_RETRIES:
        return "debug"                            # 🔧 retry path

    print(f"💀 Max retries ({MAX_RETRIES}) reached. Ending with error.")
    return END                                    # ❌ give up


# # ── Graph Builder ─────────────────────────────────────────────────────────────

def build_graph() -> StateGraph:
    reset_retries()

    g = StateGraph(GraphState)

    # Register nodes
    g.add_node("inspect", node_inspect)
    g.add_node("plan", node_plan)
    g.add_node("generate_code", node_generate_code)
    g.add_node("execute_code",    node_execute_code)
    g.add_node("debug",           node_debug)
    g.add_node("feature_eng",     node_feature_engineering)

    # Linear edges
    g.set_entry_point("inspect")
    g.add_edge("inspect", "plan")
    g.add_edge("plan", "generate_code")
    g.add_edge("generate_code", "execute_code")

    # Conditional edge after execute_code
    g.add_conditional_edges(
        "execute_code",
        route_after_execute,
        {
            "feature_eng": "feature_eng",  # Success
            "debug": "debug",  # retry
            END: END,  # max retries exceeded
        }
    )

    # After debug -> loop back to execute
    g.add_edge("debug", "execute_code")

    # After feature engineering -> done
    g.add_edge("feature_eng", END)

    return g.compile()
