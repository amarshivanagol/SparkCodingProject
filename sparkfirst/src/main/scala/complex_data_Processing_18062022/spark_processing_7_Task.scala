package complex_data_Processing_18062022

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object spark_processing_7_Task {

  case class schema(txnno: String, txndate: String, category: String, product: String)

  def main(args: Array[String]): Unit = {

    println("====started=====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    val df = spark
      .read
      .format("json")
      .option("multiline", "true")
      .load("file:///C:/Users/002NAM744/Data/JsonData/place.json")

    df.show()
    df.printSchema()

    println("===Flaten Data=====")

    val flatdf = df.select(
      col("place").alias("Place"),
      col("user.address.number").alias("Number"),
      col("user.address.pin").alias("Pin"),
      col("user.address.street").alias("Street"),
      col("user.name").alias("Name"))

    flatdf.show()
    flatdf.printSchema()

    val flatdf1 = df.select(
      col("place").alias("Place"),
      col("user.name"),
      col("user.address.*"))

    flatdf1.persist()
    flatdf1.show()
    flatdf1.printSchema()

    println("=====Done====")

  }

}
  