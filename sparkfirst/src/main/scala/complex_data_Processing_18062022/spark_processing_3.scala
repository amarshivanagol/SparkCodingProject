package complex_data_Processing_18062022

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object spark_processing_3 {

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
      .load("file:///C:/Users/002NAM744/Data/complexdataProcess/jc.json")

    df.show()
    df.printSchema()

    val flatdf = df.select(
      col("orgname"),
      col("trainer"),
      col("doorno"),
      col("address.permanent_city"),
      col("address.temporary_city"),
      col("street.permanent_street"),
      col("street.temporary_street"))

    flatdf.show()
    flatdf.printSchema()

    val complexdf = flatdf.select(
      col("orgname"),
      col("trainer"),
      col("doorno"),
      struct(

        col("permanent_city"),
        col("temporary_city"),
        col("permanent_street"),
        col("temporary_street")
        ).alias("address_street")
        )

    complexdf.show()
    complexdf.printSchema()

    complexdf.write.format("json").save("file:///C:/Users/002NAM744/Data/ComplexDataWrite/jc1")
    println("====== JSON Write done =======")
    
  }

}
  