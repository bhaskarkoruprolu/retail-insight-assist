"""
insight_agent.py

Purpose:
--------
Generate business-friendly insights from validated data.

This agent:
- interprets structured results
- applies business rules
- produces executive-style summaries
- highlights risks and trends

It NEVER queries data.
It NEVER modifies numbers.
"""

import yaml
import json
from typing import Dict, Any
import pandas as pd


# -----------------------------
# Load business rules
# -----------------------------

def load_business_rules(path: str = "schema/business_rules.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


BUSINESS_RULES = load_business_rules()


# -----------------------------
# Prompt builder
# -----------------------------

def build_insight_prompt(intent: Dict[str, Any],
                         validation: Dict[str, Any],
                         df: pd.DataFrame,
                         business_rules: dict) -> str:
    """
    Build a tightly constrained insight-generation prompt.
    """

    sample_rows = df.head(10).to_dict(orient="records")

    return f"""
You are a senior retail business analyst.

Your task is to write concise, high-signal business insights based ONLY on the provided data.

Rules:
- Do NOT invent numbers.
- Do NOT assume causes without evidence.
- Do NOT mention SQL or databases.
- Highlight risks if validation issues exist.
- Use clear executive language.
- Prefer bullet points.

User intent:
{json.dumps(intent, indent=2)}

Validation status:
{json.dumps(validation, indent=2)}

Business rules:
{json.dumps(business_rules["executive_summary"], indent=2)}

Sample result rows:
{json.dumps(sample_rows, indent=2)}

Write the final business insight.
"""


# -----------------------------
# Core insight generator
# -----------------------------

def generate_insight(intent: Dict[str, Any],
                     data_output: Dict[str, Any],
                     validation: Dict[str, Any],
                     llm_client) -> Dict[str, Any]:
    """
    Generate business insights using validated data.
    """

    df = data_output["result"]

    prompt = build_insight_prompt(intent, validation, df, BUSINESS_RULES)

    # ---- LLM call (replace with real client later) ----
    insight_text = llm_client.generate(prompt)

    return {
        "insight": insight_text,
        "row_count": data_output["row_count"],
        "issues": validation["issues"],
        "confidence": intent.get("confidence", 0.0)
    }


# -----------------------------
# Local test stub
# -----------------------------

if __name__ == "__main__":

    class DummyLLM:
        def generate(self, prompt: str) -> str:
            return """
• Category B is the top revenue contributor, outperforming Category A significantly.
• Revenue concentration suggests dependency on a limited product mix.
• No data quality risks detected in this result.
"""

    fake_df = pd.DataFrame({
        "category_clean": ["A", "B"],
        "revenue": [12000, 38000]
    })

    fake_output = {
        "result": fake_df,
        "row_count": 2
    }

    fake_intent = {
        "question_type": "ranking",
        "metrics": ["revenue"],
        "dimensions": ["category_clean"],
        "business_question": "Which category performs best?"
    }

    fake_validation = {
        "status": "pass",
        "issues": []
    }

    llm = DummyLLM()

    insight = generate_insight(fake_intent, fake_output, fake_validation, llm)
    print("\nFINAL INSIGHT:\n", insight["insight"])
