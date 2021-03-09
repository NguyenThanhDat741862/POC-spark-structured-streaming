import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.types.StructType

object UpdateMode {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Aggregations in update mode")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schema = new StructType()
      .add("Date", "string")
      .add("Open", "double")
      .add("High", "double")
      .add("Low", "double")
      .add("Close", "double")
      .add("Adj Close", "double")
      .add("Volume", "double")
      .add("Name", "string")

    val stockPricesDf = spark.readStream
      .option("header", "true")
      .option("maxFilesPerTrigger", 10)
      .schema(schema)
      .csv("file:///mnt/c/Users/USER/Desktop/POC-spark-structured-streaming/complete-mode/datasets/stock_data")

    println("-------------------------------------------------")
    println("Streaming source ready: ", stockPricesDf.isStreaming)

    stockPricesDf.printSchema()

    val averageCloseDf = stockPricesDf
      .groupBy("Name")
      .agg(avg("Close"))
      .withColumnRenamed("avg(Close)", "Average Close")

    averageCloseDf.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .option("numRows", 30)
      .start()
      .awaitTermination()
  }
}
