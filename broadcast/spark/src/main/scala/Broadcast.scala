import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col, udf}
import org.apache.spark.sql.types.StructType

object Broadcast {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Broadcast variable")
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
      .csv("file:///mnt/c/Users/USER/Desktop/POC-spark-structured-streaming/broadcast/datasets/stock_data")

    println("-------------------------------------------------")
    println("Streaming source ready: ", stockPricesDf.isStreaming)

    stockPricesDf.printSchema()

    val calculate_price_delta_udf = udf(
      (price_open: Double, price_close: Double) => { price_close - price_open } : Double
    )

    val priceDeltaBroadcastDf = broadcast(
      stockPricesDf.withColumn("PriceDelta", calculate_price_delta_udf(col("Open"), col("Close")))
    )

    val priceDeltaDf = priceDeltaBroadcastDf
      .select("Date", "Name", "PriceDelta")
      .where("PriceDelta > 10")

    priceDeltaDf.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option("numRows", 30)
      .start()
      .awaitTermination()
  }
}
