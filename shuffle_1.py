import os
import re
import json
from pyspark.sql import SparkSession

# Start Spark
spark = SparkSession.builder \
    .appName("SongVoteCount") \
    .master("local[*]") \
    .getOrCreate()

# ✅ In de container zijn dit de gemounte mappen
input_dir = "/data/input"
output_dir = "/data/output"

os.makedirs(output_dir, exist_ok=True)

for filename in os.listdir(input_dir):
    if filename.startswith("generated_votes_") and filename.endswith(".txt"):
        match = re.match(r"generated_votes_([A-Z]{2})\.txt", filename)
        if not match:
            print(f"⛔ Ongeldig bestandsformaat: {filename}")
            continue

        country_code = match.group(1)
        input_path = os.path.join(input_dir, filename)

        rdd = spark.sparkContext.textFile(input_path)

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

        output_file = os.path.join(output_dir, f"reduced_votes_{country_code}.json")
        with open(output_file, "w") as f:
            json.dump(result, f, indent=4)

        print(f"✅ Verwerking voltooid voor {country_code}. Output: {output_file}")
