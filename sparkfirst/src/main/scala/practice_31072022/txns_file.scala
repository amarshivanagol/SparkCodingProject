package practice_31072022
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object txns_file {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //Task 2------------

    /*    Read txns file ---- flat file -- so file:///c:/data/txns
    Filter Row contains Gymnastics
    print it*/
    println("==========Raw Data==========")
    val data = sc.textFile("C:///Users/002NAM744/Documents/Cloudera_13-03-2022/Data/datatxns.txt")
    println
    data.foreach(println)
    println
    println("==========Filter Gym Data==========")
    val gymdata = data.filter(x => x.contains("Gymnastics"))
    gymdata.foreach(println)
  }
}
