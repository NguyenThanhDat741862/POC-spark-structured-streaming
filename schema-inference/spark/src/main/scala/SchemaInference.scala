import org.apache.spark.sql.SparkSession

object SchemaInference {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Aggregations in update mode")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val stockPricesDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("mode", "DROPMALFORMED")
      .load("file:///mnt/c/Users/USER/Desktop/POC-spark-structured-streaming/schema-inference/datasets/stock_data")

    println("-------------------------------------------------")
    println("Streaming source ready: ", stockPricesDf.isStreaming)

    stockPricesDf.printSchema()

    stockPricesDf
      .select("Date", "Name", "Adj Close")
      .show()

    stockPricesDf
      .groupBy("Name")
      .count()
      .show()

    stockPricesDf.createOrReplaceTempView("stock_prices")

    spark.sql(
      """
        |SELECT Name, avg(Close) as avg_close
        |FROM stock_prices
        |GROUP BY Name
        |""".stripMargin)
      .show()

  }
}
