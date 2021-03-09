import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object CompleteModeSql {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Grouping in complete mode")
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
      .option("maxFilesPerTrigger", 2)
      .schema(schema)
      .csv("file:///mnt/c/Users/USER/Desktop/POC-spark-structured-streaming/complete-mode/datasets/stock_data")

    println("-------------------------------------------------")
    println("Streaming source ready: ", stockPricesDf.isStreaming)

    stockPricesDf.printSchema()

    stockPricesDf.createOrReplaceTempView("stock_prices")

    val minMaxCloseDf = spark.sql(
      """
        |SELECT
        | Name,
        | min(Close) as min_close,
        | max(Close) as max_close
        |FROM stock_prices
        |GROUP BY Name
        |""".stripMargin)

    minMaxCloseDf.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("numRows", 30)
      .start()
      .awaitTermination()

  }
}
