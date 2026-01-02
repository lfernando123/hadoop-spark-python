from pyspark import SparkContext

sc = SparkContext(appName="InDegreeDistribution")

# Load and parse edges
data = sc.textFile("hdfs://path/to/input.txt")
edges = data.filter(lambda line: not line.startswith('#')) \
            .map(lambda line: line.split('\t')) \
            .filter(lambda parts: len(parts) == 2)

# Compute in-degrees: (v, 1) -> reduceByKey
in_degrees = edges.map(lambda parts: (parts[1], 1)) \
                  .reduceByKey(lambda a, b: a + b)

# Compute distribution: (degree, 1) -> reduceByKey
distribution = in_degrees.map(lambda x: (x[1], 1)) \
                         .reduceByKey(lambda a, b: a + b)

# Save or collect results
distribution.saveAsTextFile("hdfs://path/to/output")

sc.stop()