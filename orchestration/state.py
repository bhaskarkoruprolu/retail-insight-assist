"""
state.py

Purpose:
--------
Defines the shared system state passed between all agents.
This is the single source of truth for the orchestration layer.
"""

from typing import TypedDict, Optional, Dict, Any


class AgentState(TypedDict, total=False):

    # raw user input
    user_query: str

    # intent agent output
    intent: Dict[str, Any]

    # router agent output
    routed_intent: Dict[str, Any]

    # data agent output
    data_output: Dict[str, Any]

    # validation agent output
    validation: Dict[str, Any]

    # insight agent output
    insight: Dict[str, Any]

    # control flags
    status: str                 # running | blocked | completed
    error: Optional[str]
