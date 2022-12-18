package inter_questions_skill_test
import java.time._
import java.time.format._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import java.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object ch7_questions_Date_Diff {
  def main(args: Array[String]): Unit = {
    println("====started=====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._

    val startDate = "1970-01-01"
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val oldDate = LocalDate.parse(startDate, formatter)
    val currentDate = "2015-02-25"
    val newDate = LocalDate.parse(currentDate, formatter)
    println(newDate.toEpochDay())
    println(oldDate.toEpochDay())
    println(newDate.toEpochDay() - oldDate.toEpochDay())

  }
}