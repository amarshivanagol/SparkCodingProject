

package practice_31072022
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import sys.process._
import java.util._

object spark_read_format_option_load_08102022 {

  /*  Using spark Read the format csv with some options and load the data*/

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    /*  Read CSV file to dataframe directly
    Read Parquet file to dataframe directly
    Read orc to dataframe directly
    Read json to dataframe directly
    Read avro to dataframe directly
    Read xml to dataframe directly
    Read RDBMS to dataframe directly
    Read S3 to dataframe directly
    Read hbase to dataframe directly
    Read cassandra to dataframe directly
    Read Kafka Streaming to dataframe directly
    Read azure blob to dataframe directly
    Read google to dataframe directly*/

    println("=========spark.read.formatcsv.option.load========")
    val df = spark
      .read
      .format("csv")
      .option("header", "true")
      .load("C:///Users/002NAM744/Data/sparkreadoptionload/usdata.csv")
    df.take(10).foreach(println)
    df.show()
    println("=========Process data========")
    df.createOrReplaceTempView("usdf")
    val fildf = spark.sql("select * from usdf where age > 30")
    fildf.show()
    val fildf1 = spark.sql("select upper(first_name),upper(last_name),upper(company_name),upper(address),upper(city),upper(county),upper(state),upper(zip),upper(age),upper(phone1),upper(phone2),upper(email),upper(web) from usdf where age > 30")
    fildf1.show(1000)
    println("=========Write data========")
    fildf.write.mode("overwrite").parquet("C:///Users/002NAM744/Data/sparkreadoptionload/parquetcsv")
    println("========Read print Schema=======")
    df.printSchema()
    println("========Auto casting Schema=======")

    println
    println("=========spark.read.formatjson.option.load========")

    val jsondf = spark
      .read
      .format("json")
      .load("C:///Users/002NAM744/Data/sparkreadoptionload/devices.json")

    jsondf.show(500)

    println("=========spark.read.formatparquet.option.load========")

    val parquetdf = spark
      .read
      .format("parquet")
      .load("C:///Users/002NAM744/Data/sparkreadoptionload/data.parquet")

    parquetdf.show()

    println("=========spark.read.formatorc.option.load========")

    val orcdf = spark
      .read
      .format("orc")
      .load("C:///Users/002NAM744/Data/sparkreadoptionload/orcdata.orc")

    orcdf.show()

    println("=========spark.read.formatavro.option.load========")

    val avrodf = spark
      .read
      .format("avro")
      .load("C:///Users/002NAM744/Data/sparkreadoptionload/data.avro")

    avrodf.show()

    /*    Spark uses unified formulae to read the data
    spark read format options load
    Depends on the format options changes*/

  }
}
