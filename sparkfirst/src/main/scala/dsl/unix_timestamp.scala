package dsl

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import java.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object unix_timestamp {

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

    val df = spark
      .read
      .option("header", "true")
      .format("csv")
      .load("file:///C:/Users/002NAM744/Data/allcountry(2).csv")

    df.persist()
    df.show()

    println("==== Unix_Time & Replace Check columns Task and select using selectExpr====")
    val procdata = df.selectExpr(
      "id",
      "from_unixtime(unix_timestamp(tdate,'dd-mm-yyyy'),'y') as year",
      "initcap(name) as name",
      "case when check='I' then 'INDIA' when check='K' then 'United Kingdom' else 'USA' end as check",
      "UPPER(spendby) as spendby",
      "LOWER(country) as country",
      "case when spendby='cash' then 0 else 1 end as status")

    procdata.show()
    println("====Sort by Name=====")
    procdata.orderBy(col("name").desc).show()

    println("=====Done=====")
  }
}