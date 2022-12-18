package complex_data_Processing_18062022

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object spark_processing_4 {

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
      col("name"),
      col("type"),

      col("image.height").alias("iheight"),
      col("image.url").alias("iurl"),
      col("image.width").alias("iwidth"),

      col("thumbnail.height").alias("theight"),
      col("thumbnail.url").alias("turl"),
      col("thumbnail.width").alias("twidth"))
    flatdf.show()
    flatdf.printSchema()

    val complexdf = flatdf.select(
      col("id"),
      col("name"),
      col("type"),

      struct(
        struct(
          col("iheight").alias("hieght"),
          col("iurl").alias("url"),
          col("iwidth").alias("width")).alias("image"),

        struct(
          col("theight").alias("height"),
          col("turl").alias("url"),
          col("twidth").alias("width")).alias("thumbnail")).alias("details"))
    complexdf.persist()
    complexdf.show()
    complexdf.printSchema()

  }

}
  