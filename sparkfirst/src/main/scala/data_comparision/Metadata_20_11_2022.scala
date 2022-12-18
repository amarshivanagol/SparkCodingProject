package data_comparision

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import java.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
object Metadata_20_11_2022 {

  case class schema(schemaname: String, tablename: String, columnname: String, colorder: String, datatype: String)

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

    println("================Read MAIN Metadata============")
    println
    val df1 = spark.read
      .option("header", "true")
      .format("csv")
      .load("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/Metadata/Src/MAIN_Metadata.csv")

    df1.sort(col("tablename").asc).show(1000)

    df1.select("tablename", "columnname", "tabletype")
    val MAINdf1 = df1.selectExpr("upper(tablename) as TABLE_NAME", "upper(columnname) as COLUMN_NAME", "upper(tabletype) as TABLE_TYPE")
    MAINdf1.show(100)
    println("====Sort by tablename=====")
    MAINdf1.orderBy(col("tablename").asc).show(1000)

    val MAINprocdata = df1.selectExpr(
      "upper(tablename) as TABLE_NAME",
      "upper(columnname) as COLUMN_NAME",
      "upper(tabletype) as TABLE_TYPE",
      "case when datatype='datetime' then 'TIMESTAMP_NTZ' when datatype='char' then 'TEXT' when datatype='varchar' then 'TEXT' when datatype='nvarchar' then 'TEXT' when datatype='bit' then 'BOOLEAN' when datatype='int' then 'NUMBER' when datatype='decimal' then 'NUMBER' when datatype='smallint' then 'NUMBER' when datatype='bigint' then 'NUMBER' when datatype='numeric' then 'NUMBER' when datatype='float' then 'FLOAT' when datatype='nchar' then 'TEXT' else 'None' end as DATA_TYPES")
    //when datatype='varchar' then 'TEXT' when datatype='nvarchar' then 'TEXT' when datatype='bit' then 'BOOLEAN' when datatype='int' then 'NUMBER' when datatype='decimal' then 'NUMBER' when smallint='int' then 'NUMBER' when datatype='bigint' then 'NUMBER' when datatype='numeric' then 'NUMBER' else 'None' end as datatype")
    MAINprocdata.show(1000)
    println("====Sort by tablename=====")
    MAINprocdata.orderBy(col("tablename").asc).show(1000)

    println("================Read DASP Metadata============")
    println
    val df2 = spark
      .read
      .option("header", "true")
      .format("csv")
      .load("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/Metadata/Src/Snowflake_Metadata.csv")

    df2.sort(col("tablename").asc).show(1000)
    df2.select("tablename", "columnname", "tabletype")
    val Snowdf2 = df1.selectExpr("upper(tablename) as TABLE_NAME", "upper(columnname) as COLUMN_NAME", "upper(tabletype) as TABLE_TYPE")
    println("====Sort by tablename=====")
    Snowdf2.orderBy(col("tablename").asc).show(1000)
    Snowdf2.show(100)
    /*    val columnsAll = MAINdf1.columns.map(m => col(m))
    val df1_col1 = MAINdf1.select(MAINdf1.columns.slice(1, 2).map(m => col(m)): _*).as("Col1")
    val df2_col1 = Snowdf2.select(Snowdf2.columns.slice(1, 2).map(m => col(m)): _*).as("Col2")

    df1_col1.show(1000)
    df2_col1.show(1000)*/

    val only_in_DASP = Snowdf2.except(MAINdf1)
    only_in_DASP.show(1000)

    only_in_DASP.coalesce(1).write.mode("overwrite").option("header", "true").csv("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/Metadata/only_in_DASP")

    println("================Write Difference only_in_DASP Metadata============")
    println
    val only_in_MAIN = MAINdf1.except(Snowdf2)
    only_in_MAIN.show(1000)
    println("================Write Difference only_in_MAIN Metadata============")
    println
    only_in_MAIN.coalesce(1).write.mode("overwrite").option("header", "true").csv("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/Metadata/only_in_MAIN")
    println("================Table & Columns Difference Done============")

    println
    println("================Data Types Difference Started============")
    println
    val DASPprocdata = df2.selectExpr(
      "upper(tablename) as TABLE_NAME",
      "upper(columnname) as COLUMN_NAME",
      "upper(tabletype) as TABLE_TYPE",
      "upper(datatype) as DATA_TYPES")
    //"case when datatype='datetime' then 'TIMESTAMP_NTZ' when datatype='char' then 'TEXT' when datatype='varchar' then 'TEXT' when datatype='nvarchar' then 'TEXT' when datatype='bit' then 'BOOLEAN' when datatype='int' then 'NUMBER' when datatype='decimal' then 'NUMBER' when datatype='smallint' then 'NUMBER' when datatype='bigint' then 'NUMBER' when datatype='numeric' then 'NUMBER'  else 'None' end as datatype")
    //when datatype='varchar' then 'TEXT' when datatype='nvarchar' then 'TEXT' when datatype='bit' then 'BOOLEAN' when datatype='int' then 'NUMBER' when datatype='decimal' then 'NUMBER' when smallint='int' then 'NUMBER' when datatype='bigint' then 'NUMBER' when datatype='numeric' then 'NUMBER' else 'None' end as datatype")
    DASPprocdata.show(1000)
    println("====Sort by tablename=====")
    DASPprocdata.orderBy(col("tablename").asc).show(1000)

    val Data_Types_only_in_DASP = DASPprocdata.except(MAINprocdata)
    Data_Types_only_in_DASP.sort(col("TABLE_NAME").asc).show(1000)
    Data_Types_only_in_DASP.show(1000)
    Data_Types_only_in_DASP.coalesce(1).write.mode("overwrite").option("header", "true").csv("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/Metadata/Data_Types_only_in_DASP")

    println("================Write Data Types Difference only_in_DASP Metadata============")
    println
    val Data_Types_only_in_MAIN = MAINprocdata.except(DASPprocdata)
    Data_Types_only_in_MAIN.sort(col("TABLE_NAME").asc).show(1000)
    Data_Types_only_in_MAIN.show(1000)
    println("================Write Data Types Difference only_in_MAIN Metadata============")
    println
    Data_Types_only_in_MAIN.coalesce(1).write.mode("overwrite").option("header", "true").csv("file:///C:/Users/002NAM744/Documents/Cloudera_13-03-2022/Data/Metadata/Data_Types_only_in_MAIN")
    println
    println("================Table & Columns & Data Types Difference Done============")
    println
  }
}

