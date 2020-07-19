package com.dudf.app

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import java.util.{Calendar, Date}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.dudf.bean.LiveIcFav
import com.dudf.utils.{MyEsUtil, MyKafkaUtil, OffsetManagerRedis, PropertiesUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object ForwardedKafka {
  def main(args: Array[String]): Unit = {
    //1.获得spark streaming执行环境
    val conf: SparkConf = new SparkConf().setAppName("jsonAnalysis2ES").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
    //2.得到kafka配置与kafka offsets偏移量
    val properties: Properties = PropertiesUtil.load("config.properties")
    val topic_name: String = properties.getProperty("kafka.topic")
    val group_name: String = properties.getProperty("kafka.group")
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManagerRedis.getOffset(topic_name,group_name)
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] =null

    //3.获取输入流
    if(kafkaOffsetMap!=null&&kafkaOffsetMap.size>0){
      recordInputStream = MyKafkaUtil.getKafkaStream(topic_name,ssc,kafkaOffsetMap,group_name)
    }else{
      recordInputStream = MyKafkaUtil.getKafkaStream(topic_name,ssc,group_name)
    }

    //4.周期性获取偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputStream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    val logDataStream: DStream[JSONObject] = inputGetOffsetDstream.map(x => {
      val logDataString: String = x.value()
      val logDataJson: JSONObject = JSON.parseObject(logDataString)
      val messageString: String = logDataJson.getString("message")
      val jsonObj: JSONObject = JSON.parseObject(messageString)
      jsonObj
    })

    logDataStream.foreachRDD(rdd =>{
      rdd.foreachPartition(f =>{
        val list: List[JSONObject] = f.toList
        val dList: List[(String, LiveIcFav)] = list.map(x => {
          val timestamp: String = x.getString("timestamp")
          val clientIp: String = x.getString("client_ip")
          val domain: String = x.getString("domain")
          val args: String = x.getString("args").replaceAll("params=", "")
          val argsJsonObj: JSONObject = JSON.parseObject(java.net.URLDecoder.decode(args, "UTF-8"))
          val liveIcFav = LiveIcFav(
            argsJsonObj.getString("b"),
            argsJsonObj.getString("s"),
            argsJsonObj.getString("en"),
            argsJsonObj.getString("et"),
            argsJsonObj.getString("cu"),
            argsJsonObj.getString("pu"),
            argsJsonObj.getString("uid"),
            argsJsonObj.getString("sid"),
            argsJsonObj.getString("ref"),
            argsJsonObj.getString("land"),
            argsJsonObj.getJSONObject("d").getString("gs"),
            argsJsonObj.getJSONObject("d").getString("oid"),
            argsJsonObj.getJSONObject("d").getString("st"),
            argsJsonObj.getJSONObject("d").getString("lvst"),
            timestamp,
            argsJsonObj.getJSONObject("d").getString("chl"),
            argsJsonObj.getJSONObject("d").getString("poid"),
            argsJsonObj.getJSONObject("d").getString("catId"),
            argsJsonObj.getJSONObject("d").getString("shopId")
          )
          val dt: String =System.currentTimeMillis().toString
          val randomString: String = scala.util.Random.nextInt(10000000).toString
          (dt+randomString, liveIcFav)
        })

        MyEsUtil.bulkDoc(dList,".monitoring-ls")
      })

      // 偏移量提交区
      OffsetManagerRedis.saveOffset(topic_name,group_name,offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
