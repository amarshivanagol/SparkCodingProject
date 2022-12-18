
package dsl
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import java.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object ch1_partitionby {
  //case class schema(txnno: String, txndate: String, category: String, product: String)
  //case class schema(txnno: String, txndate: String, category: String, product: String)
  def main(args: Array[String]): Unit = {
    println("====started=====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._
    //

    println("========csv========")

    //We don't have column to this data frame how we can assign column to this.
    //Using structType create a header

    val tSchema = StructType(Array(
      StructField("id", StringType, true),
      StructField("name", StringType, true),
      StructField("check", StringType, true),
      StructField("spendby", StringType, true),
      StructField("country", StringType, true)))

    val df = spark
      .read
      .schema(tSchema)
      .format("csv")
      //.option("header", "true")
      //.option("delimiter", "~")
      .load("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/allcountry.csv")
    df.show()

    df.write.
      format("csv")
      .partitionBy("country", "spendby")
      .mode("overwrite")
      .save("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/countrydf")
  }
}
