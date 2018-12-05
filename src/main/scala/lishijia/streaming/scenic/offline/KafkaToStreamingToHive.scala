package lishijia.streaming.scenic.offline

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.time.Second


object KafkaToStreamingToHive {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val Array(brokers, topics, consumer) = Array("master201", "orderTopic", "offline_consume")

    val conf = new SparkConf().setAppName("KafkaToStreamingToHive")
    val ssc = new StreamingContext(conf, Seconds(2))


    val km = new KafakManager


  }

}
