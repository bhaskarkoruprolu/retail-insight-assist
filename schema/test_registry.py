import yaml

with open("schema/registry.yaml") as f:
    schema = yaml.safe_load(f)

print(schema.keys())
print(schema["tables"].keys())

