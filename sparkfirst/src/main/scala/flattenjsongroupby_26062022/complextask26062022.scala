package flattenjsongroupby_26062022

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.io.Source

object complextask26062022 {

  case class schema(txnno: String, txndate: String,
                    custno:   String,
                    amount:   String,
                    category: String,
                    product:  String,
                    city:     String,
                    state:    String,
                    spendby:  String)

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

    val df = spark.read.format("json")
      .option("multiLine", "true")
      .load("file:///C:/Users/002NAM744/Data/actors.json")

    df.show()
    df.printSchema()

    val flattendf = df.withColumn("Actors", explode(col("Actors")))
      .withColumn("Children1", explode(col("Actors.children")))
      .select("Actors.*", "country", "version", "children1")
      .drop("children")
      .withColumnRenamed("children1", "children")

    flattendf.show()
    flattendf.printSchema()

    val complexdf1 = flattendf.groupBy("Birthdate", "Born At", "BornAt", "age", "hasChildren",
      "hasGreyHair", "name", "photo", "weight", "wife", "country", "version")
      .agg(collect_list("children").alias("children"))

    complexdf1.show()
    complexdf1.printSchema()

    val complexdf2 = complexdf1.groupBy("country", "version")
      .agg(collect_list(struct("Birthdate", "BornAt", "age", "children", "hasChildren", "hasGreyHair", "name", "photo", "weight", "wife")).alias("Actors"))
      .select("Actors", "country", "version")

    complexdf2.show()

    complexdf2.printSchema()

  }

}