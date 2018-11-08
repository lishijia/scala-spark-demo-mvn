package lishijia.spark.demo.hivejieba

import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object HiveJieba {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .registerKryoClasses(Array(classOf[JiebaSegmenter]))
        .set("spark.rpc.message.maxSize", "800")
    val spark = SparkSession
      .builder()
      .appName("Jieba udf")
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()

    def jieba_seg(df:DataFrame, columnName:String) : DataFrame = {
      val segmenter = new JiebaSegmenter()
      val seg = spark.sparkContext.broadcast(segmenter)
      val jieba_udf = udf{
        (sentence:String) =>
          val segV = seg.value
          segV.process(sentence.toString, SegMode.INDEX)
            .toArray().map(_.asInstanceOf[SegToken].word)
            .filter(_.length>1).mkString("/")
      }
      df.withColumn("seg", jieba_udf(col(columnName)))
    }

    val df =spark.sql("select sentence,label from lishijia.news_seg limit 300")
    val df_seg = jieba_seg(df,"sentence")
    df_seg.show()
    df_seg.write.mode("overwrite").saveAsTable("lishijia.news_jieba")
  }

}
