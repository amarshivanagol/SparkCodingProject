package practice_31072022
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._

object Usdata_CSV {
  //Requirement

  /*  You have a file know as usdata.csv
  Read this data
  Filter lines whose length>200
  FlatMap with delimiter ,
  Suffix zeyo to each line after the flatten
  Replace "-" with ""
  Write this data to a file
  * */

  def main(args: Array[String]): Unit = {

    println("=======Raw data=======")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.textFile("C:///Users/002NAM744/Documents/Cloudera_13-03-2022/Data/usdata.csv")
    data.take(10).foreach(println)
    println
    println("========Length data========")
    println
    val lendata = data.filter(x => x.length() > 200)
    lendata.foreach(println)

    println

    val flatdata = lendata.flatMap(x => x.split(","))
    flatdata.foreach(println)

    val name = ", zeyo"
    println
    println("========suffix data========")
    println

    val suffixdata = flatdata.map(x => x + name)
    suffixdata.foreach(println)
    println
    println("========Replace data========")
    println

    val Repdata = suffixdata.map(x => x.replace("-", ""))
    Repdata.foreach(println)

    println
    println("========Print data========")
    println
    val procdata = Repdata.coalesce(1).saveAsTextFile("C:///Users/002NAM744/Documents/Cloudera_13-03-2022/Data/repdata_25092022")

  }
}