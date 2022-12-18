package datetimestamp_legacy

import org.apache.spark.sql.functions._

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

object datatimestamp_legacy_05_09_2022 {

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
    println(spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY"))
    println(spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")) 
    //LEGACY: Spark will rebase dates/timestamps from Proleptic Gregorian calendar 
    //to the legacy hybrid (Julian + Gregorian) calendar when writing Parquet files.
    println(spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY"))
    println(spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")) 
    println(spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED"))
    println(spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED"))
    //For parquet format, there are several configurations can be used to control 
    //the behavior of date and timestamp rebase mode.
    //To use these configurations, we just need to add them when creating Spark session:
    //Based the situation, please select the right mode accordingly. 
    //Option 1: do except directly

    //sequence
    val data = Seq((1, "1200-01-01 00:00:00"), (2, "2022-06-19 00:00:00"))
    //creating dataframe
    val somDF = spark.createDataFrame(data).toDF("id", "timestampvalue")
      .withColumn("timestampvalue", col("timestampvalue").cast("timestamp"))
    somDF.printSchema()

    somDF.show()
    //Write to a folder
    somDF.write.format("parquet").mode("overwrite").save("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/tables/datetime-sparkv2")

    val df2join = spark
      .read
      .option("header", "true")
      .format("parquet")
      .load("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/tables/datetime-sparkv2")

    df2join.persist()
    df2join.show()
  }
}