import io
import os
import traceback
import json
import re
import pandas as pd
from .state import Input, Inspect, Plan, GenerateCode, ExecuteCode, FeatureEngineering, Debug
from .llm import call_llm

# --- Global retry counter ---
# Resets each time the pipeline starts fresh via reset_retries()
retry_count = 0
MAX_RETRIES = 3


def reset_retries():
    global retry_count
    retry_count = 0


# ── Node 1 ────────────────────────────────────────────────────────────────────
def inspect_dataset(state: Input) -> Inspect:
    df = pd.read_csv(state.raw_csv_path)

    shape = df.shape
    dtypes = df.dtypes.astype(str).to_dict()
    missing_pct = (df.isnull().mean() * 100).round(2).to_dict()
    basic_stats = df.describe(include="all").round(4).to_dict()

    data_profile = {
        "shape": shape,
        "dtypes": dtypes,
        "missing_pct": missing_pct,
        "basic_stats": basic_stats,
    }

    lines = []
    lines.append(f"Dataset Shape: {shape[0]} rows × {shape[1]} columns\n")
    lines.append("Column Types & Missing %:")
    for col in df.columns:
        lines.append(f"  - {col}: dtype={dtypes[col]}, missing={missing_pct[col]}%")

    lines.append("\nBasic Statistics:")
    for col, stats in basic_stats.items():
        stat_str = ", ".join(
            f"{k}={v}"
            for k, v in stats.items()
            if v is not None and str(v) != "nan"
        )
        lines.append(f"  {col}: {stat_str}")

    profile_text = "\n".join(lines)
    return Inspect(data_profile={**data_profile, "profile_text": profile_text})


# ── Node 2 ────────────────────────────────────────────────────────────────────
def plan_cleaning(state: Inspect) -> Plan:
    profile_text = state.data_profile.get("profile_text", str(state.data_profile))

    prompt = f"""You are an expert data scientist. Analyze the following dataset profile and create a step-by-step data cleaning plan.

Dataset Profile:
{profile_text}

For each column with issues, explain:
- What the problem is
- What strategy to use and why (e.g., median imputation, drop, forward-fill, encode)

Return your plan as plain text only. Do NOT wrap it in JSON or code blocks.
"""

    response = call_llm(prompt)
    return Plan(cleaning_plan=response.strip())


# ── Node 3 ────────────────────────────────────────────────────────────────────
def generate_code(state: Plan) -> GenerateCode:
    prompt = f"""You are a Python data cleaning expert. Write Python code to implement the following cleaning plan.

Cleaning Plan:
{state.cleaning_plan}

Requirements:
- Use pandas. The input CSV path is available as the variable `input_csv_path` (already defined).
- Save the cleaned dataframe to `output_csv_path` (already defined) using df.to_csv(output_csv_path, index=False).
- Do NOT include any import statements — pandas is already imported as pd.
- Do NOT wrap in a function. Write flat, executable code.

Return ONLY the Python code inside a ```python ... ``` code block. Nothing else.
"""

    response = call_llm(prompt)
    code = _extract_code(response)
    return GenerateCode(generated_code=code)


# ── Node 4 ────────────────────────────────────────────────────────────────────
def execute_code(state: GenerateCode, input_csv_path: str, output_csv_path: str) -> ExecuteCode:
    """
    Runs the generated Python code in a sandboxed namespace.
    input_csv_path and output_csv_path are injected into the exec namespace
    so the LLM-generated code can use them directly.
    Returns ExecuteCode with:
      - error=None, cleaned_csv_path set  → success
      - error=<traceback>, cleaned_csv_path=None → failure (trigger debug)
    """
    namespace = {
        "pd": pd,
        "input_csv_path": input_csv_path,
        "output_csv_path": output_csv_path,
    }

    try:
        exec(state.generated_code, namespace)  # noqa: S102
        print(f"✅ Code executed successfully. Output: {output_csv_path}")
        return ExecuteCode(error=None, cleaned_csv_path=output_csv_path)

    except Exception:
        error_msg = traceback.format_exc()
        print(f"❌ Execution error (attempt {retry_count + 1}/{MAX_RETRIES}):\n{error_msg}")
        return ExecuteCode(error=error_msg, cleaned_csv_path=None)


# ── Node 5 ────────────────────────────────────────────────────────────────────
def debug_code(state: ExecuteCode, original_code: str) -> Debug:
    """
    Reads the error traceback + the code that caused it,
    asks the LLM to fix it, and returns corrected code.
    Called only when execute_code returns an error.
    """
    global retry_count
    retry_count += 1

    print(f"🔧 Debug attempt {retry_count}/{MAX_RETRIES}...")

    prompt = f"""You are a Python debugging expert. The following code threw an error. Fix it.

--- Buggy Code ---
{original_code}

--- Error Traceback ---
{state.error}

Requirements (same as before):
- Use pandas (imported as pd). Variables `input_csv_path` and `output_csv_path` are already defined.
- Do NOT add import statements. Do NOT wrap in a function.
- Save the cleaned dataframe using df.to_csv(output_csv_path, index=False).

Return ONLY the fixed Python code inside a ```python ... ``` code block. Nothing else.
"""

    response = call_llm(prompt)
    code = _extract_code(response)
    return Debug(generated_code=code)


# ── Node 6 ────────────────────────────────────────────────────────────────────
def feature_engineering_plan(state: ExecuteCode) -> FeatureEngineering:
    """
    Runs after execute_code() succeeds.
    Reads the cleaned CSV and asks the LLM to suggest feature engineering steps.
    """
    df = pd.read_csv(state.cleaned_csv_path)

    shape = df.shape
    dtypes = df.dtypes.astype(str).to_dict()
    sample = df.head(3).to_string()

    prompt = f"""You are a machine learning expert. Given the following cleaned dataset profile, suggest a concrete feature engineering plan to improve model performance.

Dataset Shape: {shape[0]} rows × {shape[1]} columns
Column Types: {dtypes}

Sample rows:
{sample}

For each suggestion explain:
- Which column(s) to transform
- What transformation to apply (e.g., log, one-hot encode, bin, interaction feature)
- Why it would help a ML model

Return your plan as plain text only. Do NOT wrap it in JSON or code blocks.
"""

    response = call_llm(prompt)
    print("📊 Feature engineering plan ready.")
    return FeatureEngineering(feature_engineering_plan=response.strip())


# ── Helpers ───────────────────────────────────────────────────────────────────
def _extract_code(text: str) -> str:
    """
    Extract Python code from a markdown ```python ... ``` fence.
    Falls back to returning the raw text if no fence is found.
    """
    match = re.search(r"```(?:python)?\s*\n(.*?)```", text, re.DOTALL)
    if match:
        return match.group(1).strip()
    # Fallback: strip any stray fences and return as-is
    return re.sub(r"```(?:python)?", "", text).strip()


def _parse_json(text: str) -> dict:
    """
    Robustly extract a JSON object from LLM response text.
    Handles: markdown fences, unescaped newlines inside string values.
    Used only for code-returning nodes where JSON is necessary.
    """
    # Strip markdown fences
    text = re.sub(r"```(?:json)?\s*", "", text).strip()

    # Attempt 1: direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Attempt 2: sanitize unescaped control characters inside string values
    sanitized = re.sub(r'(?<!\\)\n', '\\n', text)
    sanitized = re.sub(r'(?<!\\)\t', '\\t', sanitized)
    try:
        return json.loads(sanitized)
    except json.JSONDecodeError:
        pass

    # Attempt 3: find the outermost {...} block and parse that
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            try:
                sanitized = re.sub(r'(?<!\\)\n', '\\n', match.group())
                return json.loads(sanitized)
            except json.JSONDecodeError:
                pass

    raise ValueError(f"Could not parse JSON from LLM response:\n{text[:400]}")