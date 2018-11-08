package lishijia.spark.demo.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark WordCount")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://master201:9000/lishijia/input/the_man_of_property.txt")
    lines.flatMap(x=>x.split(" "))
      .map(x=>(x,1))
      .reduceByKey((x,y)=>x+y)
      .map(x=>(x._2,x._1))
      .sortByKey(false)
      .take(10)
      .foreach(println)
  }

}
