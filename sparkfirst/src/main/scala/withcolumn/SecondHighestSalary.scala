package withcolumn


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import java.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
object SecondHighestSalary {

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
      .load("file:///C:/Users/002NAM744/Data/dept.txt")

    df.persist()
    df.show()

    //    val byDeptOrderByAssetDesc = Window
    //      .patitionBy($"department")
    //      .orderBy($"assetValue" desc)

    val byDeptOrderByAssetDesc = df.withColumn("Second_Max", dense_rank()
      .over(Window.partitionBy($"department")
       .orderBy($"salary".desc)))
      .filter("Second_Max = 2")
      .orderBy("department")
      .show()

  }

}

