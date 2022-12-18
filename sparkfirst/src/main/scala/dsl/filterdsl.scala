package dsl

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import java.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object filterdsl {
  //  case class schema(txnno: String, txndate: String, category: String, product: String)
  //
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

    //    val tschema = StructType(Array(
    //      StructField("id", StringType, true),
    //      StructField("name", StringType, true),
    //      StructField("check", StringType, true),
    //      StructField("spendby", StringType, true),
    //      StructField("country", StringType, true)))

    val df = spark
      .read
      .option("header", "true")
      .format("csv")
      .load("file:///C:/Users/002NAM744/Data/allcountry (2).csv")

    df.persist()
    df.show()

    val df1 = df.filter(col("country") === "IND")
    //1) Filter country='IND'

    val fildata = df.filter(col("country") === "IND")
    fildata.show()

    //2)  Filter country='IND' and spendby ='cash'

    val fildata1 = df.filter(
      col("country") === "IND" &&
        col("spendby") === "cash")
    fildata.show()

    //3) Filter country='IND' or spendby='cash'

    val fildata2 = df.filter(
      col("country") === "IND" ||
        col("spendby") === "cash")
    fildata.show()

    //4) Filter country not equals 'IND'

    val fildata3 = df.filter(
      !(col("country") === "IND"))
    fildata.show()

    //5)  Among 50 Countries -- filter country only IND and US
    val fildata4 = df.filter(
      col("country") isin ("IND", "US"))
    fildata.show()

    //5)  Among 50 Countries -- filter country not equals IND and US
    val fildata5 = df.filter(
      !(col("country") isin ("IND", "US")))
    fildata.show()

    //6) We have a country column but few india records IND and few with INDIA

    //Country

    //IN
    //IND
    //INDIA

    val fildata6 = df.filter(
      col("country") like ("%IN%"))
    fildata.show()

    //7) Not Like operator

    val fildata7 = df.filter(
      !(col("country") like ("%IN%")))
    fildata.show()

    df1.show()

    //you have all country.csv

    //show a dataframe with year alone in the tdate column rename it with year
    val fildata8 = df.select("id", "tdate", "name", "check", "spendby", "country")
    fildata8.show()

    val fildata9 = df.selectExpr("id", "split(tdate,'-')[2]", "name", "check", "spendby", "country")
    fildata8.show()

  }
}