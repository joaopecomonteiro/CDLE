# %%

import apache_beam as beam

import logging

import numpy as np
from collections import Counter

logging.root.setLevel(logging.ERROR)

import re
import os
import time
output_dir = "apache_beam_outputs"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
# %%

pipeline = beam.Pipeline()

inputs_pattern = 'dbpedia_csv/test.csv'


outputs = (
        pipeline
        | 'Read the dataset' >> beam.io.ReadFromText(inputs_pattern)
        | 'Write results' >> beam.io.WriteToText(output_dir+"/output1", file_name_suffix=".txt")
        | 'Print the text file name' >> beam.Map(print)
)


pipeline.run()



# %%




pipeline = beam.Pipeline()

inputs_pattern = 'dbpedia_csv/test.csv'


outputs = (
        pipeline
        | 'Read the dataset' >> beam.io.ReadFromText(inputs_pattern)
        | 'Separate to list' >> beam.Map(lambda line: line.split("\t"))
        | 'Write results' >> beam.io.WriteToText(output_dir+"/output2", file_name_suffix=".txt")
        | 'Print the text file name' >> beam.Map(print)
)


pipeline.run()



# %%


pipeline = beam.Pipeline()

inputs_pattern = 'dbpedia_csv/test.csv'


outputs = (
        pipeline
        | 'Read the dataset' >> beam.io.ReadFromText(inputs_pattern)
        | 'Separate to list' >> beam.Map(lambda line: line.split("\t"))
        | 'Find words' >> beam.FlatMap(lambda line: re.findall(r"[a-zA-Z']+", line[0]))
        | 'Write results' >> beam.io.WriteToText(output_dir+"/output3", file_name_suffix=".txt")
        | 'Print the text file name' >> beam.Map(print)
)


pipeline.run()



# %%


pipeline = beam.Pipeline()

inputs_pattern = 'dbpedia_csv/test.csv'


outputs = (
        pipeline
        | 'Read the dataset' >> beam.io.ReadFromText(inputs_pattern)
        | 'Separate to list' >> beam.Map(lambda line: line.split("\t"))
        | 'Find words' >> beam.FlatMap(lambda line: re.findall(r"[a-zA-Z']+", line[0]))
        | 'Pair words with 1' >> beam.Map(lambda word: (word, 1))
        | 'Group and sum' >> beam.CombinePerKey(sum)
        | 'Write results' >> beam.io.WriteToText(output_dir+"/output4", file_name_suffix=".txt")
        | 'Print the text file name' >> beam.Map(print)
)


pipeline.run()


# %%



import apache_beam.runners.interactive.interactive_beam as ib
ib.show_graph(pipeline)

# %%

import time

import apache_beam.runners.interactive.interactive_beam as ib
inputs_pattern = 'dbpedia_csv/test.csv'

start_time = time.time()

with beam.Pipeline() as pipeline:

    word_count = (
        pipeline
        | 'Read the dataset' >> beam.io.ReadFromText(inputs_pattern)
        | 'Separate to list' >> beam.Map(lambda line: line.split("\t"))
        | 'Find words' >> beam.FlatMap(lambda line: re.findall(r"[a-zA-Z']+", line[0]))
        | 'Pair words with 1' >> beam.Map(lambda word: (word, 1))
        | 'Group and sum' >> beam.CombinePerKey(sum)
        | 'Write results' >> beam.io.WriteToText(output_dir+"/123", file_name_suffix=".txt")
        | 'Print the text file name' >> beam.Map(print)
    )
    ib.show_graph(pipeline)

print(f"Pipeline took {time.time() - start_time} seconds")

# %%

import csv
from collections import Counter
import re

word_counts = Counter()
with open(inputs_pattern, 'r', newline='', encoding='utf-8') as file:
    csv_reader = csv.reader(file)
    for row in csv_reader:
        # Assuming each row contains one sentence
        sentence = row[2]
        # Tokenize the sentence into words
        words = re.findall(r'\b\w+\b', sentence.lower())
        # Count the occurrences of each word
        word_counts.update(words)

with open(output_dir+"/output_sequential.txt", 'w', encoding='utf-8') as file:
    for word, count in word_counts.items():
        file.write(f"{word}: {count}\n")







# %%


import time

import apache_beam.runners.interactive.interactive_beam as ib

inputs_pattern = 'dbpedia_csv/train.csv'

pipeline_time = time.time()

with beam.Pipeline() as pipeline:

    word_count = (
        pipeline
        | 'Read the dataset' >> beam.io.ReadFromText(inputs_pattern)
        | 'Separate to list' >> beam.Map(lambda line: line.split("\t"))
        | 'Find words' >> beam.FlatMap(lambda line: re.findall(r"[a-zA-Z']+", line[0]))
        | 'Pair words with 1' >> beam.Map(lambda word: (word, 1))
        | 'Group and sum' >> beam.CombinePerKey(sum)
        | 'Write results' >> beam.io.WriteToText(output_dir+"/pipeline_final", file_name_suffix=".txt")
        | 'Print the text file name' >> beam.Map(print)
    )

print(f"Pipeline took {time.time() - pipeline_time} seconds")





import csv
from collections import Counter
import re

sequential_time = time.time()

word_counts = Counter()
with open(inputs_pattern, 'r', newline='', encoding='utf-8') as file:
    csv_reader = csv.reader(file)
    for row in csv_reader:
        # Assuming each row contains one sentence
        sentence = row[2]
        # Tokenize the sentence into words
        words = re.findall(r'\b\w+\b', sentence.lower())
        # Count the occurrences of each word
        word_counts.update(words)

with open(output_dir+"/sequential_final.txt", 'w', encoding='utf-8') as file:
    for word, count in word_counts.items():
        file.write(f"{word}: {count}\n")


print(f"Sequential took {time.time() - sequential_time} seconds")



# %%
import csv
import sys

# Increase the field size limit
csv.field_size_limit(sys.maxsize)

datasets = os.listdir()
datasets = [file for file in datasets if file[-3:] == "csv"]
#print(len(datasets), datasets)

ids = np.arange(0, len(datasets), dtype=int).tolist()

pipeline_times = []
sequential_times = []

for id in range(4):
    print(f"ID -> {id}")
    chosen_datasets = datasets[:id+1]
    print(chosen_datasets)
    files_paths = [path + "/train.csv" for path in chosen_datasets]

    start_time = time.time()
    for path in files_paths:
        with beam.Pipeline() as pipeline:
            word_count = (
                    pipeline
                    | 'Read the dataset' >> beam.io.ReadFromText(path)
                    | 'Separate to list' >> beam.Map(lambda line: line.split("\t"))
                    | 'Find words' >> beam.FlatMap(lambda line: re.findall(r"[a-zA-Z']+", line[0]))
                    | 'Pair words with 1' >> beam.Map(lambda word: (word, 1))
                    | 'Group and sum' >> beam.CombinePerKey(sum)
                    | 'Write results' >> beam.io.WriteToText(output_dir + "/pipeline_all" + str(id), file_name_suffix=".txt")
                    | 'Print the text file name' >> beam.Map(print)
            )
    end_time = time.time()
    pipeline_times.append(end_time-start_time)
    print(f"Pipeline took {end_time - start_time} seconds")


    start_time = time.time()
    for path in files_paths:
        word_counts = Counter()
        with open(path, 'r', newline='', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            for row in csv_reader:
                sentence = row[2]
                words = re.findall(r'\b\w+\b', sentence.lower())
                word_counts.update(words)

        with open(output_dir + f"/sequential_final{id}.txt", 'w', encoding='utf-8') as file:
            for word, count in word_counts.items():
                file.write(f"{word}: {count}\n")

    end_time = time.time()
    sequential_times.append(end_time-start_time)
    print(f"Sequential took {end_time - start_time} seconds")
    print()


