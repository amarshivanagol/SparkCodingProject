package complex_data_Processing_18062022

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object spark_processing_6_Task {

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
      .load("file:///C:/Users/002NAM744/Data/JsonData/random_one_record.json")

    df.show()
    df.printSchema()

    println("========Explode Data===========")
    val flatdf = df.withColumn("results", explode(col("results")))
    flatdf.show()
    flatdf.printSchema()

    println("===Flaten Data=====")

    val df1 = flatdf.select(
      col("nationality"),
      col("seed"),
      col("version"),
      col("results.user.*"),
      col("results.user.location.*"),
      col("results.user.name.*"),
      col("results.user.picture.*")).drop("location", "name", "picture")

    df1.show()
    df1.printSchema()

    println("=====Done====")

  }

}
  