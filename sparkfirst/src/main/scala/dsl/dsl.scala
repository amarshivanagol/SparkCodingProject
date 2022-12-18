package dsl

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import java.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object dsl {
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
      StructField("tdate", StringType, true),
      StructField("name", StringType, true),
      StructField("check", StringType, true),
      StructField("spendby", StringType, true),
      StructField("country", StringType, true)))

    val df = spark
      .read
      .schema(tschema)
      .format("csv")
      .load("file:///C:/Users/002NAM744/Data/allcountry.csv")

    df.persist()
    df.show()

    //val df1 = df.select("id", "name", "check")

    //df1.show()
    //Read all country.csv
    //val df1 = create a new dataframe select only id,name,check
    //val df2 = create a new dataframe from df1 select only id,name
    //val df3 = create a new dataframe from df2 select only id

    //SPARK SQL
    //    df.createOrReplaceTempView("df")
    //    val df1 = spark.sql("select id,name,check from df")
    //    df1.show()
    //    df1.createOrReplaceTempView("df1")
    //    val df2 = spark.sql("select id,name from df1")
    //    df2.createOrReplaceTempView("df2")
    //    df2.show()
    //    val df3 = spark.sql("select id from df2")
    //    df3.show()

    //DSL
    //    val df1 = df.select("id", "name", "check")
    //    df1.show()
    //    val df2 = df1.select("id", "name")
    //    df2.show()
    //    val df3 = df1.select("id")
    //    df3.show()

    //Using the above dataframe -- filter country = 'IND'
    val df1 = df.filter("country='IND'")
    df1.show()

    //Using col operation filter
    val df2 = df.filter(col("country") === "IND")
    df2.show()

    /*    val df1 = df.select("id","name","check")
    df
    df1.show()*/
  }
}