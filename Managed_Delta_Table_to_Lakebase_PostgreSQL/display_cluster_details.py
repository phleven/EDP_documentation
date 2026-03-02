from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
# --- 1. Get the current cluster ID ---
# This method works when run inside a Databricks notebook/job
cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

# 1. Fetch live cluster details
cluster = w.clusters.get(cluster_id)

# 2. Fetch node type definitions to find the CPU count per node
# We filter to find the one matching the cluster's worker node type
node_types = w.clusters.list_node_types()
worker_node_info = next(n for n in node_types.node_types if n.node_type_id == cluster.node_type_id)

worker_node_info_count = len(cluster.executors)

# 3. Calculate Current Totals
# 'num_workers' reflects the current count even if autoscaling is active
current_workers = worker_node_info_count
cpus_per_worker = worker_node_info.num_cores
total_worker_cpus = current_workers * cpus_per_worker

print(f"Cluster Status: {cluster.state.value}")
print(f"Current Workers: {current_workers}")
print(f"CPUs per Worker: {cpus_per_worker}")
print(f"Total Live Worker CPUs: {total_worker_cpus}")