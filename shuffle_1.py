import os
import re
import shutil
import json
import subprocess
from pyspark.sql import SparkSession

# 🔧 Spark sessie starten
spark = SparkSession.builder \
    .appName("Shuffle1_ReduceVotes") \
    .master("local[*]") \
    .getOrCreate()

# 📂 Input en output directories (lokaal)
input_dir = os.path.expanduser("/data/input")
local_output_dir = os.path.expanduser("/data/output")
repo_3_dir = os.path.expanduser("~/EUV_TEST/euv-pipeline/repo_3_reduced_votes")

# 📁 Zorg dat directories bestaan
os.makedirs(local_output_dir, exist_ok=True)
#os.makedirs(googledrive_output_dir, exist_ok=True)
os.makedirs(repo_3_dir, exist_ok=True)

def upload_to_googledrive(local_path, remote_path):
    cmd = ["rclone", "copyto", local_path, remote_path]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"❌ Upload mislukt voor {local_path}:\n{result.stderr}")
    else:
        print(f"☁️ Upload naar Google Drive succesvol: {remote_path}")

# 🔁 Loop door alle inputbestanden
for filename in os.listdir(input_dir):
    if filename.startswith("generated_votes_") and filename.endswith(".txt"):
        match = re.match(r"generated_votes_([A-Z]{2})\.txt", filename)
        if not match:
            print(f"⛔ Ongeldig bestandsformaat: {filename}")
            continue

        country_code = match.group(1)
        input_path = os.path.join(input_dir, filename)

        # 📄 Lees data in Spark RDD
        rdd = spark.sparkContext.textFile(input_path)

        # 🔧 Mapping en reduceren
        mapped = rdd.map(lambda line: line.strip().split(",")) \
                    .filter(lambda fields: len(fields) == 4) \
                    .map(lambda fields: ((fields[0], fields[2]), 1))

        reduced = mapped.reduceByKey(lambda a, b: a + b)

        grouped = reduced.map(lambda x: (x[0][0], (x[0][1], x[1]))) \
                         .groupByKey() \
                         .mapValues(list)

        result = grouped.map(lambda x: {
            "country": x[0],
            "votes": [{"song_number": song, "count": count} for song, count in x[1]]
        }).collect()

        # 💾 Schrijf lokaal weg
        local_file = os.path.join(local_output_dir, f"reduced_votes_{country_code}.json")
        with open(local_file, "w") as f:
            json.dump(result, f, indent=4)

        # ☁️ Upload naar Google Drive via rclone
        remote_file = f"googledrive:euv-data/reduced_votes/reduced_votes_{country_code}.json"
        upload_to_googledrive(local_file, remote_file)

	# Kopiëren naar repo_3
        repo_3_file = os.path.join(repo_3_dir, f"reduced_votes_{country_code}.json")
        shutil.copy(local_file, repo_3_file)

        # ✅ Logging
        print(f"✅ {country_code} verwerkt:")
        print(f"   ➤ Lokaal bestand:       {local_file}")
        print(f"   ➤ Google Drive bestand: {remote_file}")
        print(f"   ➤ Repo_3:               {repo_3_file}")

# 🧹 Stop Spark
spark.stop()
