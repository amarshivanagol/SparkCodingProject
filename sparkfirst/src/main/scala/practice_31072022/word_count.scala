package practice_31072022
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import scala.io.Source
import spray.json._
import org.apache.spark.sql.catalyst.expressions.DateAdd

object word_count {

  /*  Using spark Read the format csv with some options and load the data*/

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    println("=========spark.read.formatcsv.option.load========")
    val df = sc.textFile("C:///Users/002NAM744/Data/sparkreadoptionload/usdata.csv")
    df.take(10).foreach(println)

    println
    println("=========Map Split========")
    val Mapdf = df.flatMap(x => x.split(",")).map(x => (x, 1))
    Mapdf.foreach(println)

    println
    println("=========Reduce By========")
    val reddf = Mapdf.reduceByKey(_ + _)
    reddf.foreach(println)

  }
}