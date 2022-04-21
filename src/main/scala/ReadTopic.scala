import org.apache.spark.sql.functions.{col, upper}
import sun.invoke.util.ValueConversions.cast

object ReadTopic extends App {

  import org.apache.spark.sql.SparkSession

  val topicInput = args(0)
  val topicOutput = args(1)

  val spark = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  val df = spark
    .readStream
    .format("kafka")
    .option("subscribe", topicInput)
    .option("kafka.bootstrap.servers", ":9092")
    .load
    .withColumn("value", upper(col("value")))

  df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", ":9092")
    .option("topic", "output")
    .option("checkpointLocation", "checkpoint")
    .start
    .awaitTermination()


}
