import requests, json
spec = requests.get("https://api.biomarkerkb.org/swagger.json", timeout=30).json()
paths = spec.get("paths", {})
print(json.dumps(sorted(paths.keys()), indent=2))
