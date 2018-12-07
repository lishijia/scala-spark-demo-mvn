package lishijia.streaming.scenic.offline

import java.util

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.time.Second


object KafkaToStreamingToHive {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val Array(brokers, topics, consumer) = Array("master201", "orderTopic", "offline_consume")

    val conf = new SparkConf().setAppName("KafkaToStreamingToHive")
    val ssc = new StreamingContext(conf, Seconds(2))

    val topicSet = topics.split(",").toSet

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
    "group.id" -> consumer)

//    val kafkaParamMap = new util.HashMap[String, String]()
//    kafkaParamMap.put("metadata.broker.list", "master201")
//    kafkaParamMap.put("group.id", "offline_consume")

    val km = new KafkaManager(kafkaParams)

    val message = km.createDirectStream(ssc, kafkaParams, topicSet)

    message.foreachRDD{rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (offsize <- offsetRanges){
        km.commitOffsetsToZK(offsetRanges)
        print(s"${offsize.topic} ${offsize.partition} " +
          s"${offsize.fromOffset}  " +
          s"${offsize.untilOffset}")
      }
    }

//    message.map(_._2).map((_,1l)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()

  }

}
