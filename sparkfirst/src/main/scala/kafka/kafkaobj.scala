package kafka
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions
import org.apache.kafka.clients.consumer._

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.types.StructType

object kafkaobj {
  def main(args: Array[String]): Unit = {

    println("Hello Stream")

    val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val ssc = new StreamingContext(conf, Seconds(2))

    val kp = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer], "group.id" -> "example",
      "auto.offset.reset" -> "earliest")

    val topics = Array("tpkstream")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent, Subscribe[String, String](
      topics,
      kp))

    stream.map(x => x.value()).print()

    ssc.start()
    ssc.awaitTermination()

  }

}