package practice_31072022

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import sys.process._
import java.util._
import org.apache.spark.sql.types._
object sparkdf_read_write_single_formulae {

  case class schmea(txnno: String, Category: String, product: String)
  def main(args: Array[String]): Unit = {

    println("========Started=========")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    /*
 *  Sql operation
 *  Spark dataframe reads =====> Single Formulae
    Spark dataframe writes ======> Single Formulae*/

    /*    Recap Scenrio
    there is data of txnsmall.txt
    Read this data using RDD
    Convert it to schema rdd
    Filter product contains "Gymnastics"
    Write the data as parquet file format*/

    val data = sc.textFile("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/txnsmall.txt")
    data.foreach(println)

    val mapsplit = data.map(x => x.split(","))
    val schemardd = mapsplit.map(x => schmea(x(0), x(1), x(2)))

    val filterdata = schemardd.filter(x => x.product.contains("Gymnastics"))
    val df = filterdata.toDF()
    df.show()

    df.coalesce(1).write.mode("overwrite").parquet("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/parquetdatas")
    println("=====done======")
    /*
    Hey Sai,

    txnsmall.txt -- can you read it as RDD
    Do Mapsplit
    Define case class
    convert to schema rdd
    convert to DF
    Apply filters on Product ===> dataframe
    Immediate convert this to a dataframe
    */
    val df1 = schemardd.toDF()
    df1.createOrReplaceTempView("txndf")
    val procdf = spark.sql("select * from txndf where product like '%Gymnastics%'")
    procdf.show()

    /*    Schemardd or rowrdd --> convert DF---> Process sql or DSL(sql knowledge required)*/

    /*    Read txnsmall.txt as RDD
    Do Mapsplit
    Convert it to row Rdd
    Define StructType
    Convert that to a dataframe
    Now filter product contains Gymnastics*/

    println("=======Row rdd==========")
    val data1 = sc.textFile("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/txnsmall.txt")
    data.foreach(println)

    val mapsplit1 = data.map(x => x.split(","))
    val rowrdd = mapsplit.map(x => Row(x(0), x(1), x(2)))
    println("========to Convert Data Frame we need Schema=====")
    val schema = StructType(Array(
      StructField("txnno", StringType, true),
      StructField("Category", StringType, true),
      StructField("product", StringType, true)))

    println("convert to Data Frame===")
    val df2 = spark.createDataFrame(rowrdd, schema)

    df2.show()

    df2.createOrReplaceTempView("rowdf")
    val procdf1 = spark.sql("select * from txndf where product like '%Gymnastics%'")
    procdf1.show()

    df.coalesce(1).write.mode("overwrite").parquet("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/parquetdatas_rowrdd")
    println("=====done======")

  }

}