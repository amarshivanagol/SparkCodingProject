package pack
import org.apache.spark._
import sys.process._


object obj { 
  

	def main(args:Array[String]):Unit={


			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")


					val data = sc.textFile("/user/cloudera/datatxns")
					val gymdata = data.filter(x=>x.contains("Gymnastics"))
					"hadoop fs -rmr /user/cloudera/datawrite".!

					gymdata.saveAsTextFile("/user/cloudera/datawrite")

					println("============done==========")


	}

  
  
}