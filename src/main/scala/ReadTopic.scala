import org.apache.spark.sql.functions.col
import sun.invoke.util.ValueConversions.cast

object ReadTopic extends App {
  import org.apache.spark.sql.SparkSession

  val topicName = args(0)

  val spark = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  val df  = spark
    .readStream
    .format("kafka")
    .option("subscribe", topicName)
    .option("kafka.bootstrap.servers", ":9092")
    .load
    .select(col("value").cast("String"))

  df.writeStream
    .format("console")
    .start.awaitTermination()

}
