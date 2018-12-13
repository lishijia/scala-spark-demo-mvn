package lishijia.streaming.scenic.offline

import java.util.Calendar

import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import lishijia.streaming.scenic.kafka.Orders
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka.KafkaManagerV1
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Streaming对接Kafka消息，然后入库到Hive中
  */
object KafkaToStreamingToHive {

  case class Order(scenic_code:String,scenic_name:String,channel_name:String,channel_code:String,order_no:String
                   ,place_time:String,settle_price:String,settle_amount:String,certificate_no:String,mobile_no:String
                   ,buy_number:String,place_year:String)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val Array(brokers, topics, consumer) =
      Array("master201:9092,slave202:9092,slave203:9092",
      "orderTopicV1",
      "offline_consume")

    val conf = new SparkConf().setAppName("KafkaToStreamingToHive").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))

    val topicSet = topics.split(",").toSet

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
    "group.id" -> consumer)

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

    ssc.start()
    ssc.awaitTermination()
  }

  def processRdd(rdd: RDD[(String, String)]): Unit = {
      val orders = rdd.map(_._2)

      orders.foreach(println)

      val df = rdd2DF(orders)
      //通过mode来指定输出文件的是append。创建新文件来追加文件
      df.write.mode(SaveMode.Append).insertInto("lishijia.s_order")
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
      val aDate = new SimpleDateFormat("yyyy-MM-dd")
      val place_time = aDate.parse(order.getPlace_time)
      val cl = Calendar.getInstance()
      cl.setTime(place_time)
      Order(order.getScenic_name, order.getChannel_name, order.getChannel_code,
        order.getOrder_no, order.getPlace_time, order.getSettle_price, order.getSettle_amount, order.getCertificate_no,
        order.getMobile_no, order.getBuy_number, order.getScenic_code,  cl.get(Calendar.YEAR)+"")
    }.toDF()
  }



}
