package practice_31072022

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import sys.process._
import java.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object Row_RDD_08102022 {

  case class schema(id: String, tdate: String, category: String, product: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._
    /*  You have datat.txt
  Read datat.txt Filter third column contains Gymnastics
  with Rowrdds
  Once done --- convert Rowrdd as Dataframe --write it as parquet
  To Process data using Rowrdd

  * 1) Take RDD{String]
  * 2) Do Mapsplit*/

    println
    println("=====Raw data======")
    val data = sc.textFile("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/datat.txt", 1)
    data.foreach(println)
    println
    println
    println("=====Map Spilt======")
    val mapspilt = data.map(x => x.split(","))
    mapspilt.foreach(println)
    println
    println
    println("=====Row RDD======")
    val rowrdd = mapspilt.map(x => Row(x(0), x(1), x(2), x(3)))
    println
    println
    rowrdd.foreach(println)
    println
    println
    println("=====filter data======")
    val filterdata = rowrdd.filter(x => x(3).toString().contains("Gymnastics"))
    filterdata.foreach(println)

    /*       Me --- Row rdd is completed. but toDF() IS not working

    spark --- you are crazy --- see first to convert to a dataframe -- U need columns right

    Me --- can I use case class  to Map Row RDD

    Spark --- No, never, impossible --- case class can be used only for schmea rdd
    Me -- who can help me?

    Spark ----More than case class there is another very powerful column definition
     StructType============================
  * */
    println("=====Define StructType schema=====")
    val schema = StructType(Array(
      StructField("id", StringType, true),
      StructField("tdate", StringType, true),
      StructField("category", StringType, true),
      StructField("product", StringType, true)))

    val df = spark.createDataFrame(filterdata, schema)
    println("=====dataframe=====")
    df.show()
    println("=====Write Parquet=====")
    df.write.mode("overwrite").parquet("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/parqdata1")
    
    println("=====Read Parquet=====")
    val newDataDF = spark.read.parquet("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/parqdata1")
    // show contents
    newDataDF.show()

    println("=====done=====")
  }
}
