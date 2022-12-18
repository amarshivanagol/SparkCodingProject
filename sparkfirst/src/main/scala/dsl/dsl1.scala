package dsl

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import java.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object dsl1 {
  case class schema(txnno: String, txndate: String, category: String, product: String)
  def main(args: Array[String]): Unit = {
    println("====started=====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._

    println("=====csv======")

    val tschema = StructType(Array(
      StructField("id", StringType, true),
      StructField("name", StringType, true),
      StructField("check", StringType, true),
      StructField("spendby", StringType, true),
      StructField("country", StringType, true)))

    val df = spark
      .read
      .option("header", "true")
      .format("csv")
      .load("file:///C:/Users/002NAM744/Data/allcountry.csv")

    df.persist()
    df.show()

    val df1 = df.filter(col("country") === "IND")

    df1.show()
  }
}