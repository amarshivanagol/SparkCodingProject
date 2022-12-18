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

object data_comparsion_BDV_22_08_2022 {

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

    //        val z = spark
    //      .read
    //      .option("header", "true")
    //      .format("csv")
    //      .load("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/Date_check.csv")
    //
    //      z.show()
    //     z.write.mode(SaveMode.Overwrite).parquet("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/Date_check")
    //

    val z1 = spark
      .read
      .option("header", "true")
      .format("csv")
      .load("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/Difference_Check/Src/L_Boeking_BDV_MAIN.csv")

    //val z1 = z.withColumn("COUNT", upper(col("COUNT"))).withColumn("Dataset_Dts", upper(col("Dataset_Dts")))

    //z.show()
    z1.createOrReplaceTempView("MAIN")
    val Maindf1 = spark.sql("""select TABLE_NAME as TABLE_NAME,CNT as COUNT from  MAIN """)
    Maindf1.show()

    val z2 = spark
      .read
      .option("header", "true")
      .format("csv")
      .load("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/Difference_Check/Src/L_Boeking_BDV_Snowflake.csv")
    z2.createOrReplaceTempView("DASP")
    val Snowdf2 = spark.sql("""select TABLE_NAME as TABLE_NAME,CNT as COUNT from  DASP """)
    Snowdf2.show()
    val inMAINNotInSnow = Maindf1.except(Snowdf2)
    val inSnowNotInMAIN = Snowdf2.except(Maindf1)

    //inMAINNotInSnow.show
    //inSnowNotInMAIN.show
    //inSnowNotInMAIN.show
    //Option 2: use GroupBy(for DataFrame with duplicate rows)
    /*
    val z1Grouped = z1.groupBy(z1.columns.map(c => z1(c)).toSeq: _*).count().withColumnRenamed("count", "recordRepeatCount")
    val z2Grouped = z2.groupBy(z2.columns.map(c => z2(c)).toSeq: _*).count().withColumnRenamed("count", "recordRepeatCount")

    z1Grouped.show
    z2Grouped.show*/

    inMAINNotInSnow.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/Difference_Check/BDV_inMAINNotInSnow_BDV")
    inSnowNotInMAIN.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/Difference_Check/BDV_inSnowNotInMAIN_BDV")

    //z1Grouped.write.mode(SaveMode.Overwrite).parquet("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/BDV_inMAINNotInSnow_BDV_Z1")
    /*    //z2Grouped.write.mode(SaveMode.Overwrite).parquet("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/BDV_inSnowNotInMAIN_BDV_Z2")
    val inZ1NotInZ2 = z1Grouped.except(z2Grouped).toDF()
    val inZ2NotInZ1 = z2Grouped.except(z1Grouped).toDF()
    inZ1NotInZ2.show
    inZ2NotInZ1.show*/
    //Option 3, use exceptAll, which should also work for data with duplicate rows

    //val inZ1NotInZ2 = z1.exceptAll(z2).toDF()
    //val inZ2NotInZ1 = z2.exceptAll(z1).toDF()
    println("================Data write Completed============")

  }
}
