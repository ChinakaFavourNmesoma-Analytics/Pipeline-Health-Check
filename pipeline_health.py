# Define a function to check if a pipeline has a WARNING
def evaluate_pipeline_health(log):
    """ checks the pipeline status"""
    # overall condition: status code must be from 200
    if log["status_code"] == 200:

        # Condition 1a: it must run between 600 and 1200 seconds
        if 600 <= log["duration_seconds"] <= 1200:
            log["health_status"] = "WARNING"

        # Condition 1b: latency is between 10 and 30 seconds
        elif 10 <= log["max_latency_seconds"] <= 30:
            log["health_status"] = "WARNING"

        # Condition 2a: if warnings exist but are just 'late data arrival'
        elif any(warning != "late data arrival" for warning in log["warnings"]):
            log["health_status"] = "WARNING"

        # Condition 2b: if it has fewer than 100 records but no errors
        elif log["record_count"] < 100 and not log["errors"]:
            log["health_status"] = "WARNING"

        # if it passes all this, then its healthy
        else:
            log["health_status"] = "HEALTHY"

    else:
        # If status_code is not 200, ignore it
        log["health_status"] = "CRITICAL"

    return log



# evaluate pipelines
def evaluate_all_pipelines(logs):
    """check all pipelines. prints pipelines that are WARNING."""
    results = []
    for log in logs:
        result = evaluate_pipeline_health(log)
        if result["health_status"] == "WARNING":  # keep WARNING pipelines
            results.append(result)
    return results


# pipelines
logs = [
    {
        "pipeline_name": "user_events_ingestion",
        "status_code": 200,
        "duration_seconds": 452,
        "record_count": 124500,
        "max_latency_seconds": 5.6,
        "errors": [],
        "warnings": ["late data arrival"],
        "ingestion_time": "2025-10-08T02:30:00Z",
        "source": "kafka"
    },
    {
        "pipeline_name": "transaction_data_load",
        "status_code": 500,
        "duration_seconds": 1300,
        "record_count": 0,
        "max_latency_seconds": 45.2,
        "errors": ["Database connection timeout"],
        "warnings": [],
        "ingestion_time": "2025-10-08T14:15:00Z",
        "source": "s3"
    },
    {
        "pipeline_name": "product_catalog_sync",
        "status_code": 200,
        "duration_seconds": 800,
        "record_count": 80,
        "max_latency_seconds": 15.0,
        "errors": [],
        "warnings": ["schema mismatch"],
        "ingestion_time": "2025-10-08T09:00:00Z",
        "source": "api"
    },
    {
        "pipeline_name": "inventory_update",
        "status_code": 200,
        "duration_seconds": 300,
        "record_count": 1500,
        "max_latency_seconds": 8.0,
        "errors": [],
        "warnings": [],
        "ingestion_time": "2025-10-08T03:45:00Z",
        "source": "ftp"
    }
]


# Run
warning_logs = evaluate_all_pipelines(logs)

print("Pipelines with WARNING status:")
for log in warning_logs:
    print(f"- {log['pipeline_name']}")

print(f"\nTotal WARNING pipelines: {len(warning_logs)}")