package com.dudf.app

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.dudf.bean.LiveIcFav
import com.dudf.utils.{MyKafkaUtil, OffsetManagerRedis, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.streaming.EsSparkStreaming

import scala.collection.mutable.ListBuffer

object ForwardedKafkaES {
  def main(args: Array[String]): Unit = {

    //1.获得spark streaming执行环境
    val conf: SparkConf = new SparkConf().setAppName("jsonAnalysis2ES").setMaster("local[*]")
    conf.set("spark.streaming.blockInterval", "50ms");
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.storage.memoryFraction", "0.4") //executor分配给缓存的内存比例，默认为0.6即60%，剩下40%为task运行的内存，实际上40%是偏小的
    conf.set("spark.locality.wait", "3000") //6000毫秒
    conf.set("spark.streaming.kafka.maxRatePerPartition", "1000") // 限制每秒钟从topic的每个partition最多消费的消息条数

    //es配置
    conf.set("es.index.auto.create", "true")
    conf.set("es.batch.size.entries", "5000")
    conf.set("es.batch.size.bytes", "2.5mb")
    conf.set("es.batch.write.refresh", "false")
    conf.set("es.nodes", "10.0.46.146:9200,10.0.46.147:9200,10.0.46.148:9200")
    conf.set("es.batch.write.refresh", "false")

    //shuffle优化
    conf.set("spark.shuffle.consolidateFiles", "true")
    conf.set("spark.reducer.maxSizeInFlight", "150m")
    conf.set("spark.shuffle.file.buffer", "128k")
    conf.set("spark.shuffle.io.maxRetries", "8")
    conf.set("spark.shuffle.io.retryWait", "6s")
    conf.set("spark.shuffle.memoryFraction", "0.3")

    val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

    //2.得到kafka配置与kafka offsets偏移量
    val properties: Properties = PropertiesUtil.load("config.properties")
    val topic_name: String = properties.getProperty("kafka.topic")
    val group_name: String = properties.getProperty("kafka.group")

    //3.获取输入流
    var recordInputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic_name,ssc,group_name)


    //4.流转化
    val logDataStream: DStream[JSONObject] = recordInputStream.map(x => {
      val logDataString: String = x.value()
      val logDataJson: JSONObject = JSON.parseObject(logDataString)
      val messageString: String = logDataJson.getString("message")
      val jsonObj: JSONObject = JSON.parseObject(messageString)
      jsonObj
    })

    //5.es-spark
    val analysisJsonStream: DStream[LiveIcFav] = logDataStream.mapPartitions(jsonObjItr => {
      val liveIcFavList = new ListBuffer[LiveIcFav]()
      val jsonList: List[JSONObject] = jsonObjItr.toList
      for (elem <- jsonList) {
        val timestamp: String = elem.getString("timestamp")
        val clientIp: String = elem.getString("client_ip")
        val domain: String = elem.getString("domain")
        val args: String = elem.getString("args").replaceAll("params=", "")
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
        liveIcFavList += liveIcFav
      }
      liveIcFavList.toIterator
    })

    analysisJsonStream.foreachRDD(rdd=>{
      saveToEs(rdd)
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def saveToEs(rdd: RDD[LiveIcFav])={
    if(!rdd.isEmpty()){
      val liveIcFav: RDD[JSONObject] = rdd.map(x => {
        val jsonObject = new JSONObject()
        jsonObject.put("oid", x.oid)
        jsonObject.put("b", x.b)
        jsonObject.put("catId", x.catId)
        jsonObject.put("chl", x.chl)
        jsonObject.put("cu", x.cu)
        jsonObject.put("en", x.en)
        jsonObject
      })
      println("aa")
      EsSpark.saveToEs(liveIcFav,"liveicfav/abc")
    }else{
      println("save to es rdd is empty")
    }
  }
}
