from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName("SparkHBaseWrite").getOrCreate()
sc = spark.sparkContext

# Example data
data = [Row(key="1", name="Alice"), Row(key="2", name="Bob")]
df = spark.createDataFrame(data)

# Debug: show dataframe
df.show()

# Get Java classes
ImmutableBytesWritable = sc._jvm.org.apache.hadoop.hbase.io.ImmutableBytesWritable
Put = sc._jvm.org.apache.hadoop.hbase.client.Put
Bytes = sc._jvm.org.apache.hadoop.hbase.util.Bytes

def to_hbase(row):
    key_str = str(row.key)   # ensure it's a string
    put = Put(Bytes.toBytes(key_str))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(row.name))
    return (ImmutableBytesWritable(Bytes.toBytes(key_str)), put)

rdd = df.rdd.map(to_hbase)

# Save to HBase
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

