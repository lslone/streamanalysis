package com.dudf.app

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.dudf.utils.{MyKafkaUtil, OffsetManagerRedis, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object AnalysisJson {
  def main(args: Array[String]): Unit = {
    //1.获得spark streaming执行环境
    val conf: SparkConf = new SparkConf().setAppName("jsonAnalysis2ES").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))
    //2.得到kafka配置,获取kafka流
    val properties: Properties = PropertiesUtil.load("config.properties")
    val topic_name: String = properties.getProperty("kafka.topic")
    val group_name: String = properties.getProperty("kafka.group")
    val kafkaOffsetMap: Map[TopicPartition, Long] = OffsetManagerRedis.getOffset(topic_name,group_name)
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] =null
    if(kafkaOffsetMap!=null&&kafkaOffsetMap.size>0){
      recordInputStream = MyKafkaUtil.getKafkaStream(topic_name,ssc,kafkaOffsetMap,group_name)
    }else{
      recordInputStream = MyKafkaUtil.getKafkaStream(topic_name,ssc)
    }

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputStream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })



    //3.数据转换
    val jsonStreaming: DStream[JSONObject] = inputGetOffsetDstream.map(x => {
      val filteredList=new ListBuffer[JSONObject]()
      val jsonString: String = x.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonString)
      val messageString: JSONObject = jsonObj.getJSONObject("message")
      messageString
    })


    jsonStreaming.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
