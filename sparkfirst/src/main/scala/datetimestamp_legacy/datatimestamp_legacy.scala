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

object datatimestamp_legacy {

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

    //sequence
    val data = Seq((1, "1450-12-25", "1800-04-26 00:00:00"), (1, "1480-12-25", "1890-04-26 00:00:00"),
      (1, "2999-09-07 00:00:00", "2999-09-07 00:00:00"), (1, "2101-01-27 00:00:00", "2101-01-27 00:00:00"),
      (1, "2101-01-27 00:00:00", "2101-01-27 00:00:00"), (1, "4014-02-06 00:00:00", "4014-02-06 00:00:00"),
      (1, "4014-02-06 00:00:00", "4014-02-06 00:00:00"),
      (1, "2121-01-11 00:00:00", "2121-01-11 00:00:00"), (1, "2300-04-01 00:00:00", "2300-04-01 00:00:00"),
      (1, "3034-06-05 00:00:00", "3034-06-05 00:00:00"))
    //creating dataframe
    val someDF = spark.createDataFrame(data).toDF("id", "datevalue", "timestampvalue")
      .withColumn("datevalue", col("datevalue").cast("date"))
      .withColumn("timestampvalue", col("timestampvalue").cast("timestamp"))
    someDF.printSchema()
    //someDF.write.format("parquet").mode("overwrite").save("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/tables/rebasemode")
    println(spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true"))
    //By default this property set to EXCEPTION
    //spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
    //spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
    //spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
    //spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    someDF.write.format("parquet").mode("overwrite").save("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/tables/rebasemode")
    //println(spark.conf.get("spark.sql.legacy.parquet.datetimeRebaseModeInWrite"))
    val df2join = spark
      .read
      .option("header", "true")
      .format("csv")
      .load("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/tables/rebasemode")

    df2join.persist()
    df2join.show()

  }

}