import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.types.StructType

object CompleteMode {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Projections in complete mode")
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

    val maxCloseDf = stockPricesDf
      .groupBy("Name")
      .agg(max("Close"))
      .withColumnRenamed("max(Close)", "Maximum Close")

    maxCloseDf.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .option("numRows", 30)
      .start()
      .awaitTermination()

  }
}
