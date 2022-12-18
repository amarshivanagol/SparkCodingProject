package sparkdf

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import java.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object obj9 {

  case class schema(txnno: String, txndate: String, category: String, product: String)
  def main(args: Array[String]): Unit = {
    println("====Started=====")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._
    val data = sc.textFile("file:///C:/Users/002NAM744/Data/datat.txt")
    data.foreach(println)
    val mapsplit = data.map(x => x.split(","))
    println("======Schemardd=======")
    val schemardd = mapsplit.map(x => schema(x(0), x(1), x(2), x(3)))
    val schemadf = schemardd.toDF()
    schemadf.show()
    schemadf.createOrReplaceTempView("txndf")
    val procdf = spark.sql("select * from txndf where product like '%Gymnastics%'")
    procdf.show()
    println("=====RowRdd======")
    val rowrdd = mapsplit.map(x => Row(x(0), x(1), x(2), x(3)))
    val simpleSchema = StructType(Array(
      StructField("txnno", StringType, true),
      StructField("txndate", StringType, true),
      StructField("category", StringType, true),
      StructField("product", StringType, true)))
    val df = spark.createDataFrame(rowrdd, simpleSchema)
    df.show()
    df.createOrReplaceTempView("rowdf")
    val filterdf = spark.sql("SELECT * from rowdf where product like '%Gymnastics%'")
    filterdf.show()

  }

}