package withcolumn

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import java.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object withColumnRenamed {

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
      .load("file:///C:/Users/002NAM744/Data/allcountry.csv")

    df.persist()
    df.show()
    val procdf = df.withColumn("tdate", expr("split(tdate,'-')[2]"))
    procdf.show()
    val procdf1 = procdf.withColumnRenamed("tdate", "year")
    procdf1.show()
    val procdf2 = procdf1.withColumn("status", expr("case when spendby='cash' then 1 else 0 end"))
    procdf2.show()
    procdf.show()
    procdf.printSchema()
  }

}