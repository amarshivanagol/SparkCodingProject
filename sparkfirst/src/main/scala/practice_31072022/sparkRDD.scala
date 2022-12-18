package practice_31072022

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object sparkRDD {
  def main(args: Array[String]): Unit = {

    println("========Started=========")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.textFile("C:///Users/002NAM744/Documents/Cloudera_13-03-2022/Data/statecity.txt")
    println("=========raw data==========")
    data.foreach(println)

    println("==========flatten data============")

    val flatdata = data.flatMap(x => x.split("~"))
    flatdata.foreach(println)

    println("======states=======")
    val states = flatdata.filter(x => x.contains("State"))
    states.foreach(println)

    println("========cities========")
    val cities = flatdata.filter(x => x.contains("City"))
    cities.foreach(println)

    println("==========final states=======")
    val finalstates = states.map(x => x.replace("State->", ""))
    finalstates.foreach(println)

    println("==========final cities=======")

    val finalcity = cities.map(x => x.replace("City->", ""))
    finalcity.foreach(println)

  }
}