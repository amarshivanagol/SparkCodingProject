package awscloudrdd_rdbms

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object awscloudrdd_rdbms {

  case class schema(txnno: String, txndate: String, category: String, product: String)

  def main(args: Array[String]): Unit = {

    println("====started=====")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    //.set("fs.s3a.access.key", "AKIA2RDQ4DHZL6CICLEO")
    //.set("fs.s3a.secret.key", "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")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    val sqldf = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://database-1.cvk5bls6gwai.ap-south-1.rds.amazonaws.com/zeyodb")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "zeyotab")
      .option("user", "root")
      .option("password", "Aditya908")
      .load()

    sqldf.show()

  }

}