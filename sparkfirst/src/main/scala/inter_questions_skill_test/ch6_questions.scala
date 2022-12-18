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
object ch6_question {
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
      .option("multiLine", true)
      .csv("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/Interview_Dataset/country.csv")

    df.show(false)
    df.printSchema()
    val df1 = df.select(col("id"), explode(split(col("country"), ",")))
    df1.show()
  }
}