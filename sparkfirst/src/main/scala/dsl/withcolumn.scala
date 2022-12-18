package dsl

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import java.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object withcolumn {
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

    val procdata = df.selectExpr(
      "id",
      "split(tdate,'-')[0] as day",
      "split(tdate,'-')[1] as month",
      "split(tdate,'-')[2] as year",
      "UPPER(name) as name",
      "initcap(check) as check",
      "spendby",
      "LOWER(country) as country",
      "case when spendby='cash' then 0 else 1 end as status")

    procdata.show()
    ////
    ////    Me ----- I have 4000 columns which consists of tdate column in middle,
    ////    I have to process only tdate split expression But using select Epr I have to select all
    //      the columns eventhough i does not have work with it.argsIs there a solution to do expression on top of only
    //      one column addressing that column without mentioning any other columns
    //
    //  spark ----- withColumn

    //withColumn(colName, expr("")
    val df1 = df.withColumn("tdate", expr("split(tdate,'-')[2]"))
      .withColumnRenamed("tdate", "year")
    df1.show()
    df.printSchema()

    val df2 = df.withColumn("txnyear", expr("split(tdate,'-')[2]"))
      .withColumnRenamed("txnyear", "year")
    df2.show()
    df.printSchema()

    //at withColName place if you give existing Column , I will impact that column directly
    //If you give non existing Column I will not Fail, instead
    //    I will perform expr for sure and attach that column at end
    //I can process only one column at time
    val df3 = df.withColumn("tdate", expr("split(tdate,'-')[2]"))
      .withColumnRenamed("tdate", "year")
      .withColumn("status", expr("case when spendby='cash' then 1 else 0 end"))

    df3.show()
    df3.printSchema()

    //val colllist = List("id", "year", "txnyear", "check")
    val df4 = df.withColumn("tdate", expr("split(tdate,'-')[2]"))
      .withColumnRenamed("tdate", "year")
      .withColumn("status", expr("case when spendby='cash' then 1 else 0 end"))
     // .select(colllist.map(col): _*)
    df4.show()
    df4.printSchema()

  }
}