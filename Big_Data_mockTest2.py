# 1. Write an SQL query to find the second-highest salary from an "Employees" table.

# SOlution

 SELECT salary FROM EMPLOYEES ORDER BY salary DESC LIMIT 1 OFFSET 1;


# Que 2. Write a MapReduce program to calculate the word count of a given input text file.

# Solution

import sys
from hadoop import HadoopJob

class WordCountMapper:

    def map(self, line):
        words = line.strip().split()
        for word in words:
            yield (word.lower(), 1)

class WordCountReducer:

    def reduce(self, word, counts):
        yield (word, sum(counts))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python word_count.py input.txt output_dir")
        sys.exit(1)

    input_file = sys.argv[1]
    output_dir = sys.argv[2]

    job = HadoopJob()
    job.set_mapper(WordCountMapper)
    job.set_reducer(WordCountReducer)
    job.set_input(input_file)
    job.set_output(output_dir)
    job.run()


# Que 3. Write a Spark program to count the number of occurrences of each word in a given text file.

# Solution - 

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

# Create a SparkSession
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read the text file
lines = spark.read.text("input.txt").rdd.map(lambda r: r[0])

# Split the lines into words
words = lines.flatMap(lambda line: line.split(" "))

# Count the occurrences of each word
word_counts = words.groupBy(lambda word: word).count()

# Sort the word counts in descending order
sorted_word_counts = word_counts.sort(col("count").desc())

# Show the result
sorted_word_counts.show()

# Stop the SparkSession
spark.stop()
