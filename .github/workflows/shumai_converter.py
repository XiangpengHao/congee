import json, os

results = []

for dir in os.listdir("target/benchmark"):
	for f in os.listdir(f"target/benchmark/{dir}"):
		if f.endswith(".json"):
			file_name = f"target/benchmark/{dir}/{f}"
			with open(file_name) as fp:
				print(f"Working on {file_name}")
				results.append(json.load(fp))

clean_results = []
for r in results:
	tmp = {"name": r["config"]["workload"],
			"unit": "QPS",
			"value": r["run"][0]["iterations"][-1]["result"]}
	clean_results.append(tmp)

json.dump(clean_results, open("output.json", "w"))