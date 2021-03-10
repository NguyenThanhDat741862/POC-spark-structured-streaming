import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object UdfsSql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Udfs using SQL")
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
      .csv("file:///mnt/c/Users/USER/Desktop/POC-spark-structured-streaming/udfs/datasets/stock_data")

    println("-------------------------------------------------")
    println("Streaming source ready: ", stockPricesDf.isStreaming)

    stockPricesDf.printSchema()

    val calculate_price_delta_udf = (price_open: Double, price_close: Double) => { price_close - price_open } : Double

    spark.udf.register("calculate_price_delta_udf", calculate_price_delta_udf)

    stockPricesDf.createOrReplaceTempView("stock_prices")

    val priceDeltaDf = spark.sql(
      """
        |SELECT
        | Date, Name, Open, Close,
        |	calculate_price_delta_udf(Open, Close) as price_delta
        | FROM stock_prices
        |""".stripMargin)

    priceDeltaDf.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option("numRows", 30)
      .start()
      .awaitTermination()

  }
}
