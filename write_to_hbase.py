from pyspark.sql import SparkSession, Row
from pyspark import SparkContext

# Initialize Spark
spark = SparkSession.builder.appName("SparkHBaseWrite").getOrCreate()
sc = spark.sparkContext

# Example data
data = [Row(key="1", name="Alice"), Row(key="2", name="Bob")]
df = spark.createDataFrame(data)

# Get Java classes via Py4J
ImmutableBytesWritable = sc._jvm.org.apache.hadoop.hbase.io.ImmutableBytesWritable
Put = sc._jvm.org.apache.hadoop.hbase.client.Put
Bytes = sc._jvm.org.apache.hadoop.hbase.util.Bytes

def to_hbase(row):
    put = Put(Bytes.toBytes(row.key))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(row.name))
    return (ImmutableBytesWritable(Bytes.toBytes(row.key)), put)

rdd = df.rdd.map(to_hbase)

rdd.saveAsNewAPIHadoopDataset(
    conf={
        "hbase.zookeeper.quorum": "hbase-zookeeper.hbase-stack.svc.cluster.local",
        "hbase.mapred.outputtable": "users",
        "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
        "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "mapreduce.job.output.value.class": "org.apache.hadoop.hbase.client.Put"
    }
)

spark.stop()
