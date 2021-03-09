import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object AppendMode {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Projections in append mode")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schema = new StructType()
      .add("Date", "string")
      .add("Open", "string")
      .add("High", "string")
      .add("Low", "string")
      .add("Close", "string")
      .add("Adj Close", "string")
      .add("Volume", "string")
      .add("Name", "string")

    val stockPricesDf = spark.readStream
      .option("header", "true")
      .schema(schema)
      .csv("file:///mnt/c/Users/USER/Desktop/POC-spark-structured-streaming/append-mode/datasets/stock_data")

    println("-------------------------------------------------")
    println("Streaming source ready: ", stockPricesDf.isStreaming)

    stockPricesDf.printSchema()

    val upDaysDf = stockPricesDf
      .select("Name", "Date", "Open", "Close")
      .where("Open > Close")

    upDaysDf.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option("numRows", 5)
      .start()
      .awaitTermination()

  }
}
