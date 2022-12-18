package data_comparision

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark._
import org.apache.spark.sql._
import com.amazonaws.protocol.StructuredPojo
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.dynamodbv2.model.BillingMode
import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import org.apache.hadoop.fs.s3a.S3AFileSystem
import com.fasterxml.jackson.dataformat.cbor.CBORFactory
import com.fasterxml.jackson.core.TSFBuilder
import org.apache.spark.streaming.kinesis.KinesisUtils
import org.apache.spark.sql.functions.upper
import org.apache.spark.sql.functions.lower
import org.apache.spark.sql.functions._

object data_comparision_metadata {

  def b2s(a: Array[Byte]): String = new String(a)

  def main(args: Array[String]): Unit = {

    println("================Data read Started============")
    println
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._
    //Option 1: do except directly

    val z1 = spark
      .read
      .option("header", "true")
      .format("csv")
      .load("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/Datatype.csv")

    //inSnowNotInMAIN.show
    //Option 2: use GroupBy(for DataFrame with duplicate rows)

    import org.apache.spark.sql.expressions.Window

// load data...


// add new column
val z = z1
            .withColumn("datatype",
                last(
                    when(
                        $"STATUS"==="TRUE" && 
                        
                        // intermediate column for comparison
                        lag($"STATUS", 1).notEqual("FALSE"),

                         concat_ws("_", $"datatype", $"data_type"))
                    .otherwise(null), true)
                .over(Window.orderBy("datatype","data_type")))
            .select("*")
    
  

    z.show
    
    
    //Option 3, use exceptAll, which should also work for data with duplicate rows

    //val inZ1NotInZ2 = z1.exceptAll(z2).toDF()
    //val inZ2NotInZ1 = z2.exceptAll(z1).toDF()
    println("================Data write Completed======================")
    
    



  }
}