package spark1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.sql.SparkSession

object obj8 {
  
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
    
    
    val schemardd = splitdata.map( x => schema(x(0),x(1),x(2),x(3)))
        
    val fildata = schemardd.filter( x => x.product.contains("Gymnastics"))
    fildata.foreach(println)
    
    println
    
    val df = fildata.toDF()
    df.show()
    
    fildata.foreach(println)
    df.write.parquet("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/DF_parq_")
  }
    
  }
  
