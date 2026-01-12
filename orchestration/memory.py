"""
memory.py

Purpose:
--------
Intent-based analytical memory.

Stores and retrieves:
- previous intents
- active analytical context
- resolved dimensions / metrics / filters
- time focus

This enables follow-up questions like:
"what about last quarter?"
"only for electronics"
"break it by region"
"""

from typing import Dict, Any, List
from copy import deepcopy


class IntentMemory:
    """
    Lightweight structured memory for analytics systems.

    This is NOT chat history.
    This is semantic analytical state.
    """

    def __init__(self, max_history: int = 5):
        self.max_history = max_history
        self.intent_history: List[Dict[str, Any]] = []
        self.active_context: Dict[str, Any] = {}

    # -----------------------------
    # Core memory operations
    # -----------------------------

    def store_intent(self, intent: Dict[str, Any]):
        """Store finalized intent into memory."""

        self.intent_history.append(deepcopy(intent))

        # keep memory bounded
        if len(self.intent_history) > self.max_history:
            self.intent_history.pop(0)

        # update active analytical context
        self._update_active_context(intent)

    def _update_active_context(self, intent: Dict[str, Any]):
        """
        Update rolling context from intent.
        This context is what follow-ups will inherit.
        """

        keys_to_track = [
            "target_tables",
            "metrics",
            "dimensions",
            "filters",
            "time_range",
            "grain"
        ]

        for key in keys_to_track:
            if intent.get(key):
                self.active_context[key] = deepcopy(intent[key])

    # -----------------------------
    # Context resolution
    # -----------------------------

    def resolve_followup(self, new_intent: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge new intent with active context.
        Only fills missing fields.
        """

        if not self.active_context:
            return new_intent

        resolved = deepcopy(new_intent)

        for key, value in self.active_context.items():
            if not resolved.get(key):
                resolved[key] = deepcopy(value)

        return resolved

    # -----------------------------
    # Introspection
    # -----------------------------

    def get_last_intent(self):
        if not self.intent_history:
            return None
        return self.intent_history[-1]

    def clear(self):
        self.intent_history.clear()
        self.active_context.clear()


# -----------------------------
# Local test
# -----------------------------

if __name__ == "__main__":

    memory = IntentMemory()

    first_intent = {
        "metrics": ["revenue"],
        "dimensions": ["category_clean"],
        "filters": {"region_clean": ["domestic"]},
        "time_range": {"period": "last_3_months"},
        "grain": "month"
    }

    memory.store_intent(first_intent)

    followup = {
        "metrics": None,
        "dimensions": ["region_clean"],
        "filters": None,
        "time_range": {"period": "last_quarter"},
        "grain": None
    }

    resolved = memory.resolve_followup(followup)

    print("\nACTIVE CONTEXT:\n", memory.active_context)
    print("\nFOLLOWUP RESOLVED:\n", resolved)
