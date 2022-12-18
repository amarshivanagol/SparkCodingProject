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

object data_comparision {

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

    val z = spark
      .read
      .option("header", "true")
      .format("csv")
      .load("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/MAIN.csv")

    val z1 = z.withColumn("tablename", upper(col("tablename"))).withColumn("columnname", upper(col("columnname")))
    
    z.show()

    val z2 = spark
      .read
      .option("header", "true")
      .format("csv")
      .load("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/Snowflake.csv")

    val inMAINNotInSnow = z1.except(z2).toDF()
    val inSnowNotInMAIN = z2.except(z1).toDF()

    inMAINNotInSnow.show
    inSnowNotInMAIN.show
    //inSnowNotInMAIN.show
    //Option 2: use GroupBy(for DataFrame with duplicate rows)

    val z1Grouped = z1.groupBy(z1.columns.map(c => z1(c)).toSeq: _*).count().withColumnRenamed("count", "recordRepeatCount")
    val z2Grouped = z2.groupBy(z2.columns.map(c => z2(c)).toSeq: _*).count().withColumnRenamed("count", "recordRepeatCount")

    z1Grouped.show
    z2Grouped.show
    
    inMAINNotInSnow.write.mode(SaveMode.Overwrite).parquet("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/inMAINNotInSnow")
    inSnowNotInMAIN.write.mode(SaveMode.Overwrite).parquet("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/inSnowNotInMAIN")
    
    val inZ1NotInZ2 = z1Grouped.except(z2Grouped).toDF()
    val inZ2NotInZ1 = z2Grouped.except(z1Grouped).toDF()
    inZ1NotInZ2.show
    inZ2NotInZ1.show
    //Option 3, use exceptAll, which should also work for data with duplicate rows

    //val inZ1NotInZ2 = z1.exceptAll(z2).toDF()
    //val inZ2NotInZ1 = z2.exceptAll(z1).toDF()
    println("================Data write Completed============")

  }
}