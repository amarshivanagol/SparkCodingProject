package practice_31072022

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object Filter_Gymnastics {

  def main(args: Array[String]): Unit = {
    println("==========started=========")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    println
    println("=========Raw data===========")
    val data = sc.textFile("C:///Users/002NAM744/Documents/Cloudera_13-03-2022/Data/datatxns.txt")
    data.foreach(println)

    //filter 4th column contains "Gymnastics"
    //spark RDD is row based process element
    //now the requirement is column based process
    //Me--spark can you please help me with column based RDD
    //Spark--I actually considers row an element -- but ur request is column based
    println
    println("========filter data=========")
    val fildata = data.filter(x => x.contains("Gymnastics"))
    fildata.foreach(println)
    
    println
    println("========flatten data===============")
    
    val flatdata = fildata.flatMap(x=>x.split(","))
    flatdata.foreach(println)
  }
}