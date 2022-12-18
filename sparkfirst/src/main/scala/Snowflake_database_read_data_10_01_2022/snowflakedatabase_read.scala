package Snowflake_database_read_data_10_01_2022


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import scala.io.Source
import spray.json._
import org.apache.spark.sql.catalyst.expressions.DateAdd

object obj {


	def jcom=udf((tj:String,yj:String)=>{


		val json1 = tj.parseJson
				val json2 = yj.parseJson
				val results=json1==json2
				results



	})



			def main(args:Array[String]):Unit={


					println("================Started1============")
					val conf = new SparkConf().setAppName("revision").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._



					val snowdf = spark.read.format("snowflake")
					.option("sfURL","https://YI78860.ap-southeast-1.snowflakecomputing.com")
					.option("sfAccount","YI78860")
					.option("sfUser","zeyoanalytics2")
					.option("sfPassword","Aditya908")
					.option("sfDatabase","zeyodb")
					.option("sfSchema","zeyoschema")
					.option("sfRole","ACCOUNTADMIN")
					.option("sfWarehouse","ZEYOWH")
					.option("query","""select * from zeyodb.zeyoschema.zeyoin where BATCHID in 
							(
							(select max(BATCHID) from zeyodb.zeyoschema.zeyoin),
							(select max(BATCHID)-1 from zeyodb.zeyoschema.zeyoin)

							)""")

					.load()


					snowdf.show(false)



					val yestbatch=snowdf.selectExpr("min(BATCHID) as BATCHID")
					yestbatch.show()

					val todaybatch=snowdf.selectExpr("max(BATCHID) as BATCHID")
					todaybatch.show()





					val yesterdaydata = snowdf.join(yestbatch,Seq("BATCHID"),"inner").drop("BATCHID")
					.withColumnRenamed("VALUE","YVALUE")
					yesterdaydata.show(false)


					val todaydata = snowdf.join(todaybatch,Seq("BATCHID"),"inner").drop("BATCHID")
					.withColumnRenamed("VALUE","TVALUE")
					todaydata.show(false)



					val joindf = yesterdaydata.join(todaydata,Seq("ID"),"full")
					joindf.show(false)



					println("====Delete====")
					println


					val deletedata = joindf.filter(col("TVALUE").isNull)
					.select("id","YVALUE")

					deletedata.show()

          println("====new====")
					println

					val newdata = joindf.filter(col("YVALUE").isNull)
					.select("id","TVALUE")

					newdata.show()



					println("====Existing and Updated==")


					val ndnn= joindf.filter(col("YVALUE").isNotNull &&
							col("TVALUE").isNotNull)


					ndnn.show(false)



					val jsoncom = ndnn.withColumn("isEqual", jcom(ndnn("YVALUE"),ndnn("TVALUE")))

					jsoncom.show()

					println("====Existing ==")

					println


					val existing = jsoncom.filter(col("isEqual")===true).select("ID","TVALUE")

					existing.show(false)


					println("====updated ==")

					println


					val updated = jsoncom.filter(col("isEqual")===false).select("ID","TVALUE")

					updated.show(false)




  					val finalnew = newdata.union(updated)
  
  
  
  					println("============final new========")
  
  					finalnew.show()


					println("====Existing ==")

					println

					existing.show(false)



					println("====Delete====")
					println
					deletedata.show()



	}

}