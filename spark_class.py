# %%

import pyspark
import pyspark.sql.functions as sf

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Pyspark Class").config("spark.memory.offHeap.enabled","true").config("spark.memory.offHeap.size","10g").getOrCreate()

# %%
import os

output_dir = "spark_outputs"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)



import csv
from collections import Counter
import re
import apache_beam as beam



import time

df_path = "yelp_review_polarity_csv/train.csv"

# %%

start_time = time.time()

train_df = spark.read.load(df_path, format="csv", sep=",", inferSchema="true")

wordCounts = train_df.select(sf.explode(sf.split(train_df["_c1"], r"[a-zA-Z']+")).alias("word")).groupBy("word").count()

with open(output_dir + f"/spark.txt", 'w', encoding='utf-8') as file:
    for word, count in wordCounts.collect():
        file.write(f"{word}: {count}\n")



print(f"\nSpark took {time.time() - start_time} seconds")

#%% APACHE BEAM

pipeline_time = time.time()

with beam.Pipeline() as pipeline:

    word_count = (
        pipeline
        | 'Read the dataset' >> beam.io.ReadFromText(df_path)
        | 'Separate to list' >> beam.Map(lambda line: line.split("\t"))
        | 'Find words' >> beam.FlatMap(lambda line: re.findall(r"[a-zA-Z']+", line[0]))
        | 'Pair words with 1' >> beam.Map(lambda word: (word, 1))
        | 'Group and sum' >> beam.CombinePerKey(sum)
        | 'Write results' >> beam.io.WriteToText(output_dir+"/pipeline_final", file_name_suffix=".txt")
        | 'Print the text file name' >> beam.Map(print)
    )

print(f"Pipeline took {time.time() - pipeline_time} seconds")

