import org.apache.spark.sql.SparkSession

object Accumulator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Accumulator")
      .getOrCreate()

    val total_acc = spark.sparkContext.longAccumulator("Total")

    spark.sparkContext.setLogLevel("ERROR")

    val rdd = spark.sparkContext.parallelize(Array(1, 2, 3, 4))

    rdd.foreach(i => total_acc.add(i))

    println(total_acc.value)
  }
}
