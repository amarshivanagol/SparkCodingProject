package sparkreadoptionload

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import java.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object readavroandavroRDD {
  case class schema(txnno: String, txndate: String, category: String, product: String)
  def main(args: Array[String]): Unit = {

    println("====started=====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .getOrCreate()

    import spark.implicits._

    println("===== AVRO FILE ======")
    println
    val avrodata = spark.read.format("avro").load("file:///C:/Users/002NAM744/Data/sparkreadoptionload/data.avro")
    avrodata.show()
    avrodata.createOrReplaceTempView("avdata")
    val filedata = spark.sql("select * from avdata where age>50")
    filedata.show()

    val rdd = filedata.rdd.map(x => x.mkString(","))
    println("====== AVRO RDD =======")
    println
    rdd.take(10).foreach(println)
    println
    println("===== Done ======")
  }
}