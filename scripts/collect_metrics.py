import time
import requests
import csv
import datetime

FLINK_REST_URL = "http://localhost:8084"
POLL_INTERVAL_SECONDS = 10

# Enter output file name
OUTPUT_CSV = "throughput.csv"

#metrics to collect
WANTED_METRICS = ["numRecordsOutPerSecond", "backPressuredTimeMsPerSecond", "numRecordsOut"]

def get_running_jobs():
    url = f"{FLINK_REST_URL}/jobs"
    resp = requests.get(url).json()
    running_jobs = [job for job in resp.get("jobs", []) if job.get("status") == "RUNNING"]
    return running_jobs

def get_job_vertices(job_id):
    url = f"{FLINK_REST_URL}/jobs/{job_id}"
    resp = requests.get(url).json()
    vertices_info = []
    for v in resp.get("vertices", []):
        vertex_id = v["id"]
        name = v["name"]
        parallelism = v["parallelism"]
        vertices_info.append((vertex_id, name, parallelism))
    return vertices_info

def collect_aggregated_metrics(job_id, vertex_id, metrics_list):
    query = ",".join(metrics_list)
    url = f"{FLINK_REST_URL}/jobs/{job_id}/vertices/{vertex_id}/subtasks/metrics?get={query}"
    resp = requests.get(url).json()
    return resp

def main():
    fieldnames = ["timestamp", "job_id", "vertex_id", "vertex_name", "metric_name", "sum"]
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        print(f"Collecting metrics (WANTED_METRICS={WANTED_METRICS}) from all running jobs every {POLL_INTERVAL_SECONDS}s.")
        print(f"Output CSV: {OUTPUT_CSV}\nPress Ctrl+C to stop.")

        while True:
            timestamp = datetime.datetime.now().isoformat()
            try:
                running_jobs = get_running_jobs()
                if not running_jobs:
                    print("No running jobs found at the moment.")
                else:
                    for job in running_jobs:
                        job_id = job["id"]
                        job_vertices = get_job_vertices(job_id)
                        for (vertex_id, vertex_name, parallelism) in job_vertices:
                            metric_data = collect_aggregated_metrics(job_id, vertex_id, WANTED_METRICS)
                            for m in metric_data:
                                row = {
                                    "timestamp": timestamp,
                                    "job_id": job_id,
                                    "vertex_id": vertex_id,
                                    "vertex_name": vertex_name,
                                    "metric_name": m["id"],
                                    "sum": m.get("sum", 0),
                                }
                                writer.writerow(row)

                f.flush()
            except Exception as e:
                print(f"Error collecting metrics: {e}")

            time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
