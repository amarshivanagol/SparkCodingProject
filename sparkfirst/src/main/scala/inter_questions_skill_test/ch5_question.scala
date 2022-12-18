package inter_questions_skill_test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import java.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
object ch5_question {

/*  
  Output
Date,Average
1/11/2022 120+90/2 (9 am current day + 3 pm previous day)
1/12/2022 110+130/2

find the average rate taking the current date 9 am and previous date 3 pm rate
*/

  
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
      .load("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/Interview_Dataset/average.csv")

    df.persist()
    df.show()

    df.select(split(col("date"), " ").getItem(0)).as("date").show()
    df.select(split(col("date"), " ").getItem(1)).as("Time").show()

    val procdata = df.select(split(col("date"), " ").getItem(0).as("Date"), split(col("date"), " ").getItem(1).as("Time"), col("Rate").cast("Int").as("Rate"))
    procdata.printSchema()
    procdata.show()

    val avg = procdata.groupBy("Date").avg("Rate").alias("avg_salary")
    avg.show()

  }
}