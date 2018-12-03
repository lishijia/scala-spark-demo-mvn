package lishijia.spark.demo.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Streaming 读取 socket 数据
  */
object SocketToStreaming {

  def main(args: Array[String]): Unit = {

    val sc = new SparkConf().setAppName("SocketToStreaming")

    // 批次执行间隔
    val ssc = new StreamingContext(sc, Seconds(2))

    // 接收来自socket的输入源数据
    val lines = ssc.socketTextStream("master201", 9999)

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map( x => (x, 1)).reduceByKey(_+_)

    // 打印每个批次单词出现的次数（此处打印单词出现的次数不包含上一个批次单词出现的此处，即不会累加上一个批次同一个单词出现的次数）
    // 如果需要累加上一个批次同一个单词出现的次数需要另做处理
    wordCounts.print()

    ssc.start()

    ssc.awaitTermination()

  }

}
