import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCount {
  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: spark-submit --class <main-class> <application-jar> <host> <port>")
      System.exit(0)
    }

    val host = args(0)
    val port = args(1).toInt

    val spark = SparkSession.builder
      .appName("Word Count")
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

    val words = readStream.select(explode(split(readStream.col("value"), " ")).alias("word"))

    val wordCounts = words
      .groupBy("word")
      .count()
      .orderBy("count")

    wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()
  }
}
