
package practice_31072022

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import sys.process._
import java.util._
import org.apache.spark.sql.types._
object rdd_task2 {
  case class schema(id: String, name: String)

  def main(args: Array[String]): Unit = {

    println("==========started===========")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    /*  Agenda

  Spark RDD Filters, Map, FlatMap
  Columns based Process Types

  Schema rdd
  RowRdd

  Spark rdd to dataframe Conversion (How, why, Scenarios)
  Schema rdd to Dataframe
  Row Rdd to Dataframe
  writing it to a parquet file*/
    /*
  Requirement:
  You have datat.txt
  Filter the 4th column contains Gymnastics
  Index always starts with zero and we need 3rd column
  Flatten it with Comma*/

    println
    println("======Raw data======")
    val data = sc.textFile("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/tx.csv", 1)
    data.take(10).foreach(println)
    
    //--contains double quote filter
    val data1 = data.filter(x=>x.contains("\"")) //2,"zeyobron,analytics"
    data1.foreach(println)
    val data2 = data.filter(x => !(x.contains("\"")))//1,Sai

//    val data1map = data1.map(x=>x.split(",")).map(x=>schema((x(0),x(1)+","x(2)))
//    val data2map = data2.map(x=>x.split(",")).map(x=>schema((x(0),x(1)))
//    val finalrdd = data1map.union(data2map)
//    finalrdd.foreach(println)
            
    /*    println
    println("=========filter data========")
    val fildata = data.filter(x => x.contains("Gymnastics"))
    fildata.foreach(println)*/

//    println
//    println("=========flatten data=========")
//    val data1map = data.map(x => x.split(",")).map(x = schema(x(0),x(1)+",",x(2)))
//    data2map.foreach(println)

    /*      Me -- spark can you help me with column based rdd processing
    spark -- I actually considers row an element --
    but ur request is column based
    filters - I accept your request -- but its not possible directly -- You have do
      some processing of data
      Me -- what are the steps
      spark -- You can achieve it with two ways.
      lets talk about of the First way

      1) Take RDD[String]
      2) Since you expect column based filter first split each, delimiter
      3) Impose Column to the each Split
      4) Using the column name filter it out -------- schemaRDD
     (Column imposed RDD)
      Spark -- In scala you have a benefit of define the columns --- case class but that case class should be
      outside your main method

      Two ways

1) Schema rdd
2)


How to achieve it using schema rdd

1) Take rdd[String]
2) Do mapsplit with delimiter (,)
3) Define Columns/Schema using case class outside the main method
4) Import columns/schema to this split rdd
5) Filter using columns
      */

//    val schemardd = splitdata.map(x => schema(x(0), x(1))
//
//    val fildata = schemardd.filter(x => x.product.contains("Gymnastics"))
//    fildata.foreach(println)
//    println
//    val df = fildata.toDF()
//    df.show()
//    df.write.parquet("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/parqdata")
//    println("=========done==========")
  }
}