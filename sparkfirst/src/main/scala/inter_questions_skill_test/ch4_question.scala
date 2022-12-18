package inter_questions_skill_test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import sys.process._
import java.util._

object ch4_question {

  /*  Using spark Read the format csv with some options and load the data*/

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    println("=========spark.read.formatcsv.option.load========")
    val df = spark
      .read
      .format("csv")
      //.option("header", "true")
      .load("C:///Users/002NAM744/Documents/Cloudera_13-03-2022/Data/Req1.txt")
    df.take(10).foreach(println)
    df.show()

//    //val unpi = df.select($"_c0", expr("stack(3,'_c1',_c1,'_c2',_c2,'_c3',_c3)as(id,dept)")).where("dept is not null")
//      //.drop("id").withColumnRenmaed("_c0", "id")
//
//    println()
//    println("------output File-----")
//    unpi.show()

  }
}