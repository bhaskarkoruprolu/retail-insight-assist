"""
intent_agent.py

Purpose:
--------
Translate natural language business questions into structured analytics intent.

This agent:
- understands the question
- maps it to schema registry concepts
- outputs a strict intent object
- detects ambiguity

It does NOT touch data.
"""

import json
import yaml
from typing import Dict, Any


# -----------------------------
# Load schema registry
# -----------------------------

def load_schema_registry(path: str = "schema/registry.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


SCHEMA = load_schema_registry()


# -----------------------------
# Intent object template
# -----------------------------

INTENT_TEMPLATE = {
    "question_type": None,        # aggregation | comparison | ranking | trend | summary
    "business_question": None,

    "target_tables": [],
    "metrics": [],
    "dimensions": [],
    "filters": {},

    "time_range": {
        "type": None,
        "start": None,
        "end": None,
        "period": None,
        "comparison": None
    },

    "grain": None,
    "expected_output": None,

    "ambiguities": [],
    "confidence": 0.0,
    "out_of_scope": False
    
}


# -----------------------------
# Prompt builder (STRICT)
# -----------------------------

def build_intent_prompt(user_query: str, schema: dict) -> str:
    return f"""
You are a STRICT analytics intent extraction engine.

Your task is to convert a business question into a VALID analytics intent JSON.

CRITICAL RULES (DO NOT VIOLATE):

1. If the question asks to compare entities
   (e.g. "which category", "top product", "highest revenue by region",
    "best performing brand"),
   YOU MUST include that entity in the "dimensions" field.
   NEVER return an empty dimensions list for comparison or ranking questions.

2. Metrics are numeric values to aggregate (e.g. revenue, orders).
3. Dimensions are entities being grouped or compared
   (e.g. category_clean, region_clean, sku).
4. NEVER invent fields, columns, or metrics.
5. If required information is missing, list it in "ambiguities".
6. NEVER answer the business question.
7. Output ONLY valid JSON. No explanations. No markdown.

Available tables:
{list(schema["tables"].keys())}

Supported metrics:
{list(schema["metrics"].keys())}

Supported question types:
{schema["question_types"]}

JSON STRUCTURE (must match exactly):

{json.dumps(INTENT_TEMPLATE, indent=2)}

User question:
\"\"\"{user_query}\"\"\"
"""


# -----------------------------
# Core intent resolver
# -----------------------------

def resolve_intent(user_query: str, llm_client) -> Dict[str, Any]:
    """
    Convert user query into structured analytics intent.
    """

    prompt = build_intent_prompt(user_query, SCHEMA)

    response_text = llm_client.generate(prompt)

    # -----------------------------
    # Strict JSON extraction
    # -----------------------------

    start = response_text.find("{")
    end = response_text.rfind("}")

    if start == -1 or end == -1:
        raise ValueError("No JSON object found in LLM response")

    clean_json = response_text[start:end + 1]

    try:
        intent = json.loads(clean_json)
    except Exception as e:
        raise ValueError("Intent agent failed to return valid JSON") from e

    # -----------------------------
    # Out-of-domain detection
    # -----------------------------

    allowed_question_types = SCHEMA.get("question_types", [])

    if intent.get("question_type") not in allowed_question_types:
        intent["ambiguities"].append(
            "Question is outside the supported analytics domain"
        )
        intent["out_of_scope"] = True
    else:
        intent["out_of_scope"] = False

    
    # -----------------------------
    # Post-parse safety checks
    # -----------------------------

    intent["business_question"] = user_query

    if not intent.get("question_type"):
        intent["ambiguities"].append("Unable to detect question type")

    if not intent.get("metrics"):
        intent["ambiguities"].append("No metric detected")

    if intent["question_type"] in ["comparison", "ranking"] and not intent.get("dimensions"):
        intent["ambiguities"].append(
            "Comparison question requires at least one dimension"
        )

    if not intent.get("target_tables"):
        intent["ambiguities"].append("No target table detected")

    return intent


# -----------------------------
# Local test stub
# -----------------------------

if __name__ == "__main__":

    class DummyLLM:
        def generate(self, prompt: str) -> str:
            return json.dumps({
                "question_type": "ranking",
                "business_question": "",
                "target_tables": ["fact_sales", "dim_product"],
                "metrics": ["revenue"],
                "dimensions": ["category_clean"],
                "filters": {},
                "time_range": {
                    "type": "relative",
                    "start": None,
                    "end": None,
                    "period": "last_3_months",
                    "comparison": None
                },
                "grain": "month",
                "expected_output": "ranked_list",
                "ambiguities": [],
                "confidence": 0.85
            })

    llm = DummyLLM()
    query = "Which category generated the highest revenue in the last 3 months?"

    intent = resolve_intent(query, llm)
    print(json.dumps(intent, indent=2))
