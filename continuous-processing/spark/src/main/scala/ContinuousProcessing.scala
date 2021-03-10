import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object ContinuousProcessing {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Continuous processing")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val streamDf = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .option("rampUpTime", 1)
      .load()

    println("-------------------------------------------------")
    println("Streaming source ready: ", streamDf.isStreaming)

    val selectDf = streamDf.selectExpr("*")

    selectDf.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.Continuous("1 second"))
      .start()
      .awaitTermination()

  }
}
