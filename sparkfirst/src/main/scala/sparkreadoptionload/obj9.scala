package sparkreadoptionload

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import java.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object obj9 {
  case class schema(txnno: String, txndate: String, category: String, product: String)
  def main(args: Array[String]): Unit = {

    println("====started=====")
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._

    println
    println("=====Read csvdata======")
    val csvdf = spark.read.format("csv").option("header", "true").load("file:///C:/Users/002NAM744/Data/sparkreadoptionload/usdata.csv")
    csvdf.show()

    println
    println("=====Read jsondata======")
    val jsondf = spark.read.format("json").load("file:///C:/Users/002NAM744/Data/sparkreadoptionload/devices.json")
    jsondf.show()

    println
    println("=====Read parquetdata======")
    val parquetdf = spark.read.format("parquet").load("file:///C:/Users/002NAM744/Data/sparkreadoptionload/data.parquet")
    parquetdf.show()

    println
    println("=====Read orcdata======")
    val orcdf = spark.read.format("orc").load("file:///C:/Users/002NAM744/Data/sparkreadoptionload/orcdata.orc")
    orcdf.show()

    println
    println("=====Read avrodata======")
    val avrodf = spark.read.format("avro").load("file:///C:/Users/002NAM744/Data/sparkreadoptionload/data.avro")
    avrodf.show()

    csvdf.createOrReplaceTempView("usdf")
    val fildf = spark.sql("select * from usdf where age>30")
    fildf.show()
    fildf.write.format("csv").mode("overwrite").save("file:///C:/Users/002NAM744/Data/writedf/csvdata")
    fildf.write.format("parquet").mode("overwrite").save("file:///C:/Users/002NAM744/Data/writedf/paruetdata")
    fildf.write.format("json").mode("overwrite").save("file:///C:/Users/002NAM744/Data/writedf/jsondata")
    fildf.write.format("orc").mode("overwrite").save("file:///C:/Users/002NAM744/Data/writedf/orcdata")
    fildf.write.format("avro").mode("overwrite").save("file:///C:/Users/002NAM744/Data/writedf/avrodata")

    println("=====data written=======")

    println("=====Read csvdata======")

    fildf.write.format("csv").save("file:///C:/Users/002NAM744/Data/sparkreadoptionload/data/datawritescsv") //new directory

    //fildf.write.format("csv").mode("error").save("file:///C:/Users/002NAM744/Data/sparkreadoptionload/data/datawritescsv") //already exist

    //fildf.write.format("csv").mode("append").save("file:///C:/Users/002NAM744/Data/sparkreadoptionload/data/datawritescsv") // append the data

    fildf.write.format("csv").mode("overwrite").save("file:///C:/Users/002NAM744/Data/sparkreadoptionload/data/datawritescsv") // overwrites the data

    //fildf.write.format("csv").mode("ignore").save("file:///C:/Users/002NAM744/Data/sparkreadoptionload/data/datawritescsv") // no change time of file generated

    println("======Default read format=======")

    val csvfile = spark.read.load("file:///C:/Users/002NAM744/Data/writedf/paruetdata")
    csvfile.show()
    val avrofile = spark.read.load("file:///C:/Users/002NAM744/Data/writedf/jsondata")
    //avrofile.show()
    val orcfile = spark.read.load("file:///C:/Users/002NAM744/Data/writedf/orcdata")
    //orcfile.show()
    val jsonfile = spark.read.load("file:///C:/Users/002NAM744/Data/writedf/avrodata")
    //jsonfile.show()

  }
}