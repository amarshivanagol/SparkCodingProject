package practice_31072022

import org.apache.spark._
import sys.process._
import org.apache.spark.sql._

object Index {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("f").master("local[*]")
      .getOrCreate()

    import spark.implicits._
    println("==========started===============")
    println("==========Raw data==============")
    val liststr = List(
      "State->TN~City->Chennai",
      "State->Gujarat~City->GandhiNagar").toDF()
    //liststr.foreach(println)
    println

    liststr.show()

    liststr.createOrReplaceTempView("lisstr")
    val df1 = spark.sql("select instr(value,'->')from lisstr")

    df1.show()

    /*    val splitlist = liststr.flatMap(x => x.split("~"))
    val filstate = splitlist.filter(x => x.contains("State"))
    val filcity = splitlist.filter(x => x.contains("City"))
    println("==========Flatten data State==============")
    filstate.foreach(println)
    println
    println("==========Flatten data City==============")
    filcity.foreach(println)
    println
    println("==========Replace Final data State==============")
    val repstate = filstate.map(x => x.replace("State->", ""))
    repstate.foreach(println)
    println
    println("==========Replace Final data City==============")
    val repcity = filcity.map(x => x.replace("City->", ""))
    repcity.foreach(println)
    println
    //val state = List("TN","Gujarat")
    //val city = List("Chennai","GandhiNagar")
*/

  }
}
