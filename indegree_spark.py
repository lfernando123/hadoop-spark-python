from pyspark import SparkContext

sc = SparkContext(appName="InDegreeDistribution")

input_path = "hdfs://namenode:9000/input/soc-pokec.txt"
output_path = "hdfs://namenode:9000/output/pokec_spark"

# Load and parse edges
data = sc.textFile(input_path)

edges = (
    data
    .filter(lambda line: not line.startswith('#'))
    .map(lambda line: line.split())
    .filter(lambda parts: len(parts) >= 2)
)

# In-degree: (destination, 1)
in_degrees = (
    edges
    .map(lambda parts: (parts[1], 1))
    .reduceByKey(lambda a, b: a + b)
)

# Distribution: (degree, 1)
distribution = (
    in_degrees
    .map(lambda x: (x[1], 1))
    .reduceByKey(lambda a, b: a + b)
)

# Save result
distribution.saveAsTextFile(output_path)

sc.stop()
