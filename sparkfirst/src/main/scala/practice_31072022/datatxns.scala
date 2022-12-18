package practice_31072022
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object datatxns {
  def main(args: Array[String]): Unit = {

    //Requirement
    /*  We have data known as datatxns.txt
  Read this data
  Iterate each element filter row contains Gymnastics word*/

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.textFile("C:///Users/002NAM744/Documents/Cloudera_13-03-2022/Data/datatxns.txt")
    println("============raw data===========")
    data.foreach(println)

    println("=========Gym data===========")
    val gymdata = data.filter(x => x.contains("Gymnastics"))
    gymdata.foreach(println)
    
    println("=========Load data===========")
    gymdata.coalesce(1).saveAsTextFile("C:///Users/002NAM744/Documents/Cloudera_13-03-2022/Data/procgym")
    println
    println("=========Load data completed===========")
    println
  }
}