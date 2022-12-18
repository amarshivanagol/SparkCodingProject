package join

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import java.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object join {

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

    //val listcol = List("", "")

    val df1join = spark
      .read
      .option("header", "true")
      .format("csv")
      .load("file:///C:/Users/002NAM744/Data/jn1.txt")

    val df2join = spark
      .read
      .option("header", "true")
      .format("csv")
      .load("file:///C:/Users/002NAM744/Data/jn2.txt")

    df1join.persist()
    df1join.show()
    df2join.persist()
    df2join.show()

    println("===== Inner Join ======")
    val innerjoindf = df1join.join(df2join, Seq("id"), "inner")
    innerjoindf.show()

    println("===== left Join ======")
    val leftdf = df1join.join(df2join, Seq("id"), "left")
    leftdf.show()

    println("===== left Join ======")
    val rightdf = df1join.join(df2join, Seq("id"), "right")
    rightdf.show()

    println("===== left anti df ======")
    val leftantidf = df1join.join(df2join, Seq("id"), "left")
    rightdf.show()

    println("===== full outer Join ======")
    val outerdf = df1join.join(df2join, Seq("id"), "right")
    outerdf.show()

    println("===== Join Using different id ======")
    val diffid = df1join.join(df2join, df1join("id") === df2join("id"), "left_anti")
    diffid.show()
  }

}