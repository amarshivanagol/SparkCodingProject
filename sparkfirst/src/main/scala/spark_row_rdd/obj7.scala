package spark_row_rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import java.util._
import sys.process._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object obj7 {
  
  case class schema(id:String , tdate:String, category:String, product:String)
  
  def main(args:Array[String]):Unit={
    
    println("====started=====")
    
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark  = SparkSession
    .builder()
    .getOrCreate()
    
    import spark.implicits._
    
    println
    println("====Raw data=====")
    val data = sc.textFile("file:///C:/Users/002NAM744/Data/datat.txt",1)
    data.foreach(println)
    
    println
    val splitdata = data.map( x => x.split(","))
    
    
    val rowrdd = splitdata.map( x => Row(x(0),x(1),x(2),x(3)))
        
    val fildata = rowrdd.filter( x => x(3).toString().contains("Gymnastics"))
    fildata.foreach(println)
    
    val schema = StructType(Array(
        StructField("id",StringType,true),
        StructField("tdate",StringType,true),
        StructField("category",StringType,true),
        StructField("product",StringType,true)
        ))
    
    println
    
    val df = spark.createDataFrame(fildata, schema)
    
    df.show()
    
    fildata.foreach(println)
    df.write.mode("overwrite").parquet("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/DF_parq_row_rdd_")
  }
    
  }
  
