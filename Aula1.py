# %%

import apache_beam as beam

import logging
logging.root.setLevel(logging.ERROR)

import re

# %%

pipeline = beam.Pipeline()

inputs_pattern = 'dbpedia_csv/test.csv'


outputs = (
        pipeline
        | 'Read the dataset' >> beam.io.ReadFromText(inputs_pattern)
        | 'Write results' >> beam.io.WriteToText("output1", file_name_suffix=".txt")
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
        | 'Write results' >> beam.io.WriteToText("output2", file_name_suffix=".txt")
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
        | 'Write results' >> beam.io.WriteToText("output3", file_name_suffix=".txt")
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
        | 'Write results' >> beam.io.WriteToText("output4", file_name_suffix=".txt")
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
        | 'Write results' >> beam.io.WriteToText("123", file_name_suffix=".txt")
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

with open("output_sequential.txt", 'w', encoding='utf-8') as file:
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
        | 'Write results' >> beam.io.WriteToText("pipeline_final", file_name_suffix=".txt")
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

with open("sequential_final.txt", 'w', encoding='utf-8') as file:
    for word, count in word_counts.items():
        file.write(f"{word}: {count}\n")


print(f"Sequential took {time.time() - pipeline_time} seconds")









