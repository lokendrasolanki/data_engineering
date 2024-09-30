# from pyspark.sql import SparkSession
import re
from collections import Counter

# Initialize Spark session
# spark = SparkSession.builder.appName("LogProcessingExample").getOrCreate()

# Sample log data
log_data = [
    "192.168.1.1 - - [01/Jul/2021:12:00:00 +0000] \"GET /index.html HTTP/1.1\" 200 2326",
    "192.168.1.2 - - [01/Jul/2021:12:01:00 +0000] \"POST /login HTTP/1.1\" 302 185",
    "192.168.1.1 - - [01/Jul/2021:12:02:00 +0000] \"GET /about.html HTTP/1.1\" 200 1234",
    "192.168.1.3 - - [01/Jul/2021:12:03:00 +0000] \"GET /products HTTP/1.1\" 200 5678",
    "192.168.1.2 - - [01/Jul/2021:12:04:00 +0000] \"GET /index.html HTTP/1.1\" 200 2326",
    "192.168.1.4 - - [01/Jul/2021:12:05:00 +0000] \"POST /purchase HTTP/1.1\" 200 152",
]

# Create RDD from log data
log_rdd = spark.sparkContext.parallelize(log_data)

# Regular expression to parse log entries
log_pattern = r'(\S+) - - \[(.*?)\] "(.*?)" (\d+) (\d+)'

def process_log_partition(logs):
    # Initialize counters for this partition
    status_counts = Counter()
    total_bytes = 0
    
    parsed_logs = []
    
    for log in logs:
        match = re.match(log_pattern, log)
        if match:
            ip, timestamp, request, status, bytes_sent = match.groups()
            
            # Extract method and path from the request
            method, path, _ = request.split()
            
            # Update counters
            status_counts[status] += 1
            total_bytes += int(bytes_sent)
            
            # Create a structured log entry
            parsed_log = {
                "ip": ip,
                "timestamp": timestamp,
                "method": method,
                "path": path,
                "status": int(status),
                "bytes_sent": int(bytes_sent)
            }
            parsed_logs.append(parsed_log)
    
    # Yield the parsed logs and partition-level statistics
    yield {
        "logs": parsed_logs,
        "status_counts": dict(status_counts),
        "total_bytes": total_bytes
    }

# Apply mapPartitions to process logs
processed_rdd = log_rdd.mapPartitions(process_log_partition)

# Collect results
results = processed_rdd.collect()

# Process and display results
for partition_result in results:
    print("Parsed Logs:")
    for log in partition_result["logs"]:
        print(f"  {log}")
    print("\nPartition Statistics:")
    print(f"  Status Counts: {partition_result['status_counts']}")
    print(f"  Total Bytes Sent: {partition_result['total_bytes']}")
    print("\n" + "="*50 + "\n")

# Create a DataFrame from the parsed logs
logs_df = spark.createDataFrame([log for result in results for log in result["logs"]])

# Show the resulting DataFrame
print("Resulting of the DataFrame:")
logs_df.show(truncate=False)