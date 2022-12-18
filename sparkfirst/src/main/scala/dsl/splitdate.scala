package dsl

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import java.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object splitdate {
  //case class schema(txnno: String, txndate: String, category: String, product: String)
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
      .option("header", "true")
      .format("csv")
      .load("file:///C:/Users/002NAM744/Data/allcountry(2).csv")

    df.persist()
    df.show()

    val procdata = df.selectExpr("id", 
                                "split(tdate,'-')[0] as day",
                                "split(tdate,'-')[1] as month",
                                "split(tdate,'-')[2] as year",
                                 "UPPER(name) as name", 
                                 "initcap(check) as check", 
                                 "spendby", 
                                 "LOWER(country) as country",
                                 "case when spendby='cash' then 0 else 1 end as status")

    procdata.show()
  }
}