import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object Checkpointing {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Checkpointing")
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
      .schema(schema)
      .csv("file:///mnt/c/Users/USER/Desktop/POC-spark-structured-streaming/checkpointing/datasets/stock_data")

    println("-------------------------------------------------")
    println("Streaming source ready: ", stockPricesDf.isStreaming)

    stockPricesDf.printSchema()

    stockPricesDf.createOrReplaceTempView("stock_prices")

    val avgCloseDf = spark.sql(
      """
        |SELECT Name, avg(Close) as avg_close
        |FROM stock_prices
        |GROUP BY Name
        |""".stripMargin)

    avgCloseDf.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("numRows", 30)
      .option("checkpointLocation", "checkpoint")
      .start()
      .awaitTermination()
  }
}
