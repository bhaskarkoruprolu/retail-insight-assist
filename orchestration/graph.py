"""
graph.py

Purpose:
--------
LangGraph orchestration for Retail Insights Assistant.

Flow:
User Query
   ↓
Intent Agent
   ↓
Router Agent
   ↓
Data Agent
   ↓
Validation Agent
   ↓
(if pass/warn)
Insight Agent
   ↓
Final Output
"""

from langgraph.graph import StateGraph, END

from orchestration.state import AgentState
from orchestration.memory import IntentMemory

from agents.intent_agent import resolve_intent
from agents.router import route_intent
from agents.data_agent import execute_intent
from agents.validation_agent import validate_result
from agents.insight_agent import generate_insight


# -----------------------------
# LLM wrappers
# -----------------------------

from llm.llm_client import LLMClient

llm_client = LLMClient(model="gpt-4o-mini", temperature=0)



# -----------------------------
# Agent wrapper nodes
# -----------------------------

memory = IntentMemory(max_history=5)

def intent_node(state: AgentState) -> AgentState:
    raw_intent = resolve_intent(state["user_query"], llm_client)

    # Stop early if out of scope
    if raw_intent.get("out_of_scope"):
        return {
            "intent": raw_intent,
            "status": "blocked",
            "error": "Out-of-scope question"
        }

    resolved_intent = memory.resolve_followup(raw_intent)
    return {"intent": resolved_intent}



def router_node(state: AgentState) -> AgentState:
    routed = route_intent(state["intent"])
    return {"routed_intent": routed}


def data_node(state: AgentState) -> AgentState:
    try:
        output = execute_intent(state["routed_intent"])
        return {"data_output": output}

    except ValueError as e:
        # Gracefully handle blocked / out-of-scope queries
        return {
            "status": "blocked",
            "error": str(e)
        }



def validation_node(state: AgentState) -> AgentState:
    verdict = validate_result(state["routed_intent"], state["data_output"])

    if verdict["status"] != "block":
        # store only valid analytical context
        memory.store_intent(state["intent"])

    status = "blocked" if verdict["status"] == "block" else "running"

    return {
        "validation": verdict,
        "status": status
    }



def insight_node(state: AgentState) -> AgentState:
    insight = generate_insight(
        intent=state["intent"],
        data_output=state["data_output"],
        validation=state["validation"],
        llm_client=llm_client
    )

    return {
        "insight": insight,
        "status": "completed"
    }


# -----------------------------
# Conditional routing
# -----------------------------

def validation_router(state: AgentState) -> str:
    if state["status"] == "blocked":
        return "end"
    return "insight"

def data_router(state: AgentState) -> str:
    if state.get("status") == "blocked":
        return "end"
    return "validation"


def scope_router(state: AgentState) -> str:
    if state.get("status") == "blocked":
        return "end"
    return "router"

# -----------------------------
# Build LangGraph
# -----------------------------

def build_graph():

    graph = StateGraph(AgentState)

    graph.add_node("intent", intent_node)
    graph.add_node("router", router_node)
    graph.add_node("data", data_node)
    graph.add_node("validation", validation_node)
    graph.add_node("insight", insight_node)

    graph.set_entry_point("intent")

    graph.add_edge("intent", "router")
    
    graph.add_conditional_edges(
        "intent",
        scope_router,
        {
            "router": "router",
            "end": END
        }
    )
    
    graph.add_edge("router", "data")
    
    graph.add_conditional_edges(
        "validation",
        validation_router,
        {
            "insight": "insight",
            "end": END
        }
    )
    
    graph.add_conditional_edges(
        "data",
        data_router,
        {
            "validation": "validation",
            "end": END
        }
    )

    graph.add_edge("insight", END)
    
    

    return graph.compile()


# -----------------------------
# Local test runner
# -----------------------------

if __name__ == "__main__":

    app = build_graph()

    result = app.invoke({
        "user_query": "Which category generated the highest revenue last quarter?"
    })

    print("\n--- FINAL SYSTEM OUTPUT ---\n")
    print(result)

#Restart memory after UI session ends

def reset_memory():
    memory.clear()