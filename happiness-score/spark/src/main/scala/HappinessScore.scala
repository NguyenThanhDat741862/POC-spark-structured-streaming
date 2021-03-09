import org.apache.spark.sql.SparkSession

object HappinessScore {
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: spark-submit --class <main-class> <application-jar> <host> <port>")
      System.exit(0)
    }

    val host = args(0)
    val port = args(1).toInt

    val spark = SparkSession.builder
      .appName("Happiness Score")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val readStream = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()

    println("-------------------------------------------------")
    println("Streaming source ready: ", readStream.isStreaming)

    readStream.printSchema()

    val readStreamDf = readStream.selectExpr(
      "split(value, ',')[0] as Country",
      "split(value, ',')[1] as Region",
      "split(value, ',')[2] as HappinessScore"
    )

    readStreamDf.createOrReplaceTempView("happiness")

    val averageScoreDf = spark.sql(
      """
        |SELECT
        | Region,
        | AVG(HappinessScore) as avgScore
        |FROM Happiness
        |GROUP BY Region
        |""".stripMargin)

    averageScoreDf.writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()
  }
}
