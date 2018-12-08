package lishijia.streaming.scenic.offline

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import kafka.serializer.StringDecoder
import lishijia.streaming.scenic.kafka.Orders
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager, KafkaManagerV1, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.time.Second

//https://blog.csdn.net/duan_zhihua/article/details/52006054?locationNum=11
//https://blog.csdn.net/ligt0610/article/details/47311771
//https://www.jianshu.com/p/2369a020e604
//http://www.zhangrenhua.com/2016/08/02/hadoop-spark-streaming%E6%95%B0%E6%8D%AE%E6%97%A0%E4%B8%A2%E5%A4%B1%E8%AF%BB%E5%8F%96kafka%EF%BC%8C%E4%BB%A5%E5%8F%8A%E6%BA%90%E7%A0%81%E5%88%86%E6%9E%90/
object KafkaToStreamingToHive {

  case class Order(scenic_code:String,scenic_name:String,channel_name:String,channel_code:String,order_no:String
                   ,place_time:String,settle_price:String,settle_amount:String,certificate_no:String,mobile_no:String
                   ,buy_number:String,place_year:String)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val Array(brokers, topics, consumer) =
      Array("hadoop101:9092,hadoop102:9092,hadoop103:9092",
      "orderTopic",
      "offline_consume")

    val conf = new SparkConf().setAppName("KafkaToStreamingToHive").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))

    val topicSet = topics.split(",").toSet

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
    "group.id" -> consumer)
//    val kafkaParamMap = new util.HashMap[String, String]()
//    kafkaParamMap.put("metadata.broker.list", "master201")
//    kafkaParamMap.put("group.id", "offline_consume")

    val km = new KafkaManagerV1(kafkaParams)
    val message = km.createDirectStream[
      String,
      String,
      StringDecoder,
      StringDecoder](ssc, kafkaParams, topicSet)

    message.foreachRDD(rdd => {
        // 先处理消息
         processRdd(rdd)
        // 再更新offsets
         km.updateZKOffsets(rdd)
    })

    message.foreachRDD{rdd=>
//      print("this is froeach rdd")
//      //val orderDf = rdd2DF(rdd)
//      //orderDf.write.mode(SaveMode.Append).insertInto("lishijia.s_order")
//      rdd.map{ x=>
//          print(x)
//      }
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for(offsize <- offsetRanges){
        km.commitOffsetsToZK(offsetRanges)
        println(s"${offsize.topic} ${offsize.partition} ${offsize.fromOffset}  ${offsize.untilOffset}")
        //        badou 0 2798598  2798627
      }
      println()
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def processRdd(rdd: RDD[(String, String)]): Unit ={
    rdd.map{ x =>
      println(x._2)
    }
  }

  /**
    * @param rdd
    * @return
    */
  def rdd2DF(rdd:RDD[String]): DataFrame = {
    val spark = SparkSession
      .builder()
      .appName("KafkaDataToHive")
      .config("hive.exec.dynamic.parition", "true")
      .config("hive.exec.dynamic.parition.mode", "nonstrict")
      .enableHiveSupport().getOrCreate()
    import spark.implicits._
    /**
      * {"scenic_code":"s0001", "scenic_name":"景区s0001", "channel_name":"销售渠道c001", "
      * channel_code":"c001","order_no":"s000100000001", "place_time":"2018-12-05 12:23:12", "
      * settle_price":"110.00", "settle_amount":"110.00", "certificate_no":"430502xxxx06176212",
      * "mobile_no":"136xxxx6224", "buy_number":"1", }
      */
    rdd.map{x=>
      val order = JSON.parseObject(x, classOf[Orders])
      import java.text.SimpleDateFormat
      val aDate = new SimpleDateFormat("yyyy")
      val place_year = aDate.format(new Date(order.getPlace_time))
      Order(order.getScenic_code, order.getScenic_name, order.getChannel_name, order.getChannel_code,
        order.getOrder_no, order.getPlace_time, order.getSettle_price, order.getSettle_amount, order.getCertificate_no,
        order.getMobile_no, order.getBuy_number, place_year)
    }.toDF()
  }

}
