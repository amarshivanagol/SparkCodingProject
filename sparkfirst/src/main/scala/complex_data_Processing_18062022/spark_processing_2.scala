package complex_data_Processing_18062022

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object spark_processing_2 {

  case class schema(txnno: String, txndate: String, category: String, product: String)

  def main(args: Array[String]): Unit = {

    println("====started=====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    val df = spark
      .read
      .format("json")
      .option("multiline", "true")
      .load("file:///C:/Users/002NAM744/Data/JsonData/donut.json")

    df.show()
    df.printSchema()

    val flatdf = df.select(
      col("id"),
      col("image.height").alias("iheight"),
      col("image.url").alias("iurl"),
      col("image.width").as("iwidth"),
      col("name"),
      col("thumbnail.height").alias("theight"),
      col("thumbnail.url").alias("turl"),
      col("thumbnail.width").alias("twidth"),
      col("type"))

    flatdf.show()
    flatdf.printSchema()

    val filterdata = flatdf.filter(col("iheight") === 20)

  }

}
  