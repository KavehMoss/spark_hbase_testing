from pyspark.sql import SparkSession, Row

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SparkHBaseWrite") \
    .getOrCreate()

# Example data
data = [Row(key="1", name="Alice"),
        Row(key="2", name="Bob"),
        Row(key="3", name="Charlie")]
df = spark.createDataFrame(data)

# Convert to HBase key-value format
def to_hbase(row):
    return (row.key, {"info:name": row.name})

rdd = df.rdd.map(to_hbase)

# Save to HBase using Hadoop API
rdd.saveAsNewAPIHadoopDataset(
    conf={
        "hbase.zookeeper.quorum": "hbase-zookeeper.hbase-stack.svc.cluster.local",
        "hbase.mapred.outputtable": "users",
        "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
        "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"
    }
)

spark.stop()

