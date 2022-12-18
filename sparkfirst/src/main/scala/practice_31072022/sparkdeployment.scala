package practice_31072022

import org.apache.spark._
import sys.process._

object sparkdeployment {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("first").setMaster("Local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    val data = sc.textFile("/user/cloudera/datatxns")
    val gymdata = data.filter(x => x.contains("Gymnastics"))
    "hadoop fs rm -r /user/cloudera/datawritegym".!
    gymdata.saveAsTextFile("user/cloudera/datawritegym")
  }

}
