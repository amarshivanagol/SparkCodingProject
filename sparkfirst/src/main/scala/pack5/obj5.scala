package pack5

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._

object obj5 {

  def main(args: Array[String]): Unit = {

    println("====started=====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.textFile("file:///C:/Users/002NAM744/Data/usdata.csv")
    println("====raw=data=====")
    data.take(10).foreach(println)

    println
    println("====Length data=====")
    println

    val lendata = data.filter(x => x.length() > 200)
    lendata.foreach(println)

    println
    println("====flat data=====")
    println

    val flatdata = lendata.flatMap(x => x.split(","))
    flatdata.foreach(println)

    val name = ",zeyo"
    println
    println("====suffix data=====")
    println

    val suffixdata = flatdata.map(x => x + name)
    suffixdata.foreach(println)

    println
    println("====replace data=====")
    println

    val repdata = suffixdata.map(x => x.replace("-", ""))
    repdata.foreach(println)

    repdata.coalesce(1).saveAsTextFile("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/repdata")

    println
    println("====Done=====")
    println

  }

}


//Task 1 ------
//
//
//Create 5 projects in Eclipse  or Intellij
//project1 (pack1,obj1),project2(pack2,obj2),project3,project4,project5
//change java, add scala nature, create package,create Obj, Add spark Jars, define main method
//
//
//Task 2 ------ 
//
//Read txns file    --- flat file -- so file:///C:/data/txns
//Filter Row contains Gymnastics
//print it