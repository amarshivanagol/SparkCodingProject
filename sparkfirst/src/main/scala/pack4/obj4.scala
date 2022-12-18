package pack4

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._

object obj4 {

  def main(args: Array[String]): Unit = {

    println("====started=====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

   // val data = sc.textFile("file:///C:/Users/002NAM744/Data/datatxns.txt") // change this path according to you
   
    val data = sc.textFile("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/datatxns.txt")
    println("====rawdata=====-")
    data.foreach(println)
    val gymdata = data.filter(x => x.contains("Gymnastics"))
    println("====proc data=====-")
    gymdata.foreach(println)

  }
}
  