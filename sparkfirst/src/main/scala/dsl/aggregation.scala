package dsl

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import java.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object aggregation {
  //case class schema(txnno: String, tdate: String, amount:float, category: String, product: String)
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

    val df = spark
      .read
      .option("header", true)
      .format("csv")
      .load("file:///C:/Users/002NAM744/Data/datatxns (2).txt")

    df.persist()
    df.show()

    val aggdf = df.groupBy(
      "category",
      "product")
      .agg(
        sum("amount").alias("total"), count("*")
          .alias("count_rec"))
    aggdf.show()

  }
}  