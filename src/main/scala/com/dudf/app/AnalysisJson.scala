package com.dudf.app

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.dudf.utils.{JSONUtils, MyKafkaUtil, OffsetManagerRedis, PropertiesUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import scala.collection.mutable.ListBuffer

object AnalysisJson {
  def main(args: Array[String]): Unit = {
    //1.获得spark streaming执行环境
    val conf: SparkConf = new SparkConf().setAppName("jsonAnalysis2ES").setMaster("local[*]")
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
      recordInputStream = MyKafkaUtil.getKafkaStream(topic_name,ssc)
    }

    //4.周期性获取偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordInputStream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    //5.类型转换
    /*val jsonStreaming: DStream[JSONObject] = inputGetOffsetDstream.map(x => {
      val filteredList=new ListBuffer[JSONObject]()
      val jsonString: String = x.value()
      /*var jsonObj: JSONObject =
      if(JSONUtils.isJSONString(jsonString)){
        val jsonObj: JSONObject = JSON.parseObject(jsonString)
      }*/

      val messageString: JSONObject = jsonObj.getJSONObject("message")
      println(messageString)
      messageString
    })*/

/*    val jsonObjDstream: DStream[JSONObject] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonString)
      val ts: lang.Long = jsonObj.getLong("ts")
      val datehourString: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
      val dateHour: Array[String] = datehourString.split(" ")

      jsonObj.put("dt", dateHour(0))
      jsonObj.put("hr", dateHour(1))

      jsonObj
    }*/

    //6.流内容转换
    /*val filteredDstream: DStream[JSONObject] = inputGetOffsetDstream.mapPartitions { jsonObjItr =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val filteredList=new ListBuffer[JSONObject]()
      //  Iterator 只能迭代一次 包括取size   所以要取size 要把迭代器转为别的容器
      val jsonList: List[ConsumerRecord[String, String]] = jsonObjItr.toList
      //   println("过滤前："+jsonList.size)
      for (jsonObj <- jsonList) {
        jsonObj
        val dt: String = jsonObj.getString("dt")
        val mid: String = jsonObj.getJSONObject("common").getString("mid")
        val dauKey = "dau:" + dt
        val isNew: lang.Long = jedis.sadd(dauKey, mid) //如果未存在则保存 返回1  如果已经存在则不保存 返回0
        jedis.expire(dauKey,3600*24)
        if (isNew == 1L) {
          filteredList+=jsonObj
        }
      }
      jedis.close()
      // println("过滤后："+filteredList.size)
      filteredList.toIterator
    }

    filteredDstream.foreachRDD{rdd=>
      rdd.foreachPartition{jsonItr=>
        val list: List[JSONObject] = jsonItr.toList
        //把源数据 转换成为要保存的数据格式
        val dauList: List[(String,DauInfo)] = list.map { jsonObj =>
          val commonJSONObj: JSONObject = jsonObj.getJSONObject("common")
          val dauInfo = DauInfo(commonJSONObj.getString("mid"),
            commonJSONObj.getString("uid"),
            commonJSONObj.getString("ar"),
            commonJSONObj.getString("ch"),
            commonJSONObj.getString("vc"),
            jsonObj.getString("dt"),
            jsonObj.getString("hr"),
            "00",
            jsonObj.getLong("ts")
          )

          (dauInfo.mid,dauInfo)

        }
        val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        MyEsUtil.bulkDoc(dauList,"gmall0105_dau_info_"+dt)

      }
      ///
      // 偏移量提交区
      OffsetManager.saveOffset(topic,groupId,offsetRanges)
      ///

    }
    jsonStreaming.print()*/
    ssc.start()
    ssc.awaitTermination()
  }

}
