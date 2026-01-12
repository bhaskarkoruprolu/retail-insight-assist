from orchestration.graph import build_graph

app = build_graph()

app.invoke({"user_query": "Which category has highest revenue?"})
app.invoke({"user_query": "What about last quarter?"})
app.invoke({"user_query": "Only international"})