package com.dudf.app

import com.alibaba.fastjson.{JSON, JSONObject}

object demo {
  def main(args: Array[String]): Unit = {
    val logDataString = "{\"@timestamp\":\"2020-07-17T15:26:02.880Z\",\"@metadata\":{\"beat\":\"filebeat\",\"type\":\"_doc\",\"version\":\"7.3.0\",\"topic\":\"ods_log\"},\"log\":{\"file\":{\"path\":\"/data/nginx/logs-uat.weilaijishi.com_access.log\"},\"offset\":3038},\"message\":\"{\\\"timestamp\\\":\\\"2020-07-17T23:25:53+08:00\\\",\\\"client_ip\\\":\\\"223.74.151.67\\\",\\\"method\\\":\\\"GET\\\",\\\"status\\\":\\\"304\\\",\\\"domain\\\":\\\"logs-uat.weilaijishi.com\\\",\\\"http_referer\\\":\\\"-\\\",\\\"request_id\\\":\\\"a67f9e2e5779b60a4e360455ee0f9933\\\",\\\"agent\\\":\\\"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36\\\",\\\"uri\\\":\\\"/v.gif\\\",\\\"args\\\":\\\"params={%22b%22:%22ZB%22,%22s%22:%22LL%22,%22en%22:%22ic%22,%22et%22:%22FAV%22,%22cu%22:%22%22,%22pu%22:%22%22,%22uid%22:%22%22,%22sid%22:%22%22,%22ref%22:%22%22,%22land%22:%22%22,%22d%22:{%22gs%22:%22%22,%22ob%22:%22%22,%22sq%22:%22%22,%22oid%22:%22live001%22,%22st%22:%2212341234234%22,%22lvst%22:%220%22,%22lvo%22:%22WU007%22,%22chl%22:%22JD%22,%22poid%22:%22SKU0001%22,%22catId%22:%22100%22,%22shopId%22:%22s002%22,%22pr%22:%22%22,%22pt%22:%22%22,%22so%22:%22m%22,%22amt%22:%22%22,%22sum%22:%22%22}}\\\"}\",\"input\":{\"type\":\"log\"},\"ecs\":{\"version\":\"1.0.1\"},\"host\":{\"architecture\":\"x86_64\",\"os\":{\"name\":\"CentOS Linux\",\"kernel\":\"3.10.0-957.21.3.el7.x86_64\",\"codename\":\"Core\",\"platform\":\"centos\",\"version\":\"7 (Core)\",\"family\":\"redhat\"},\"id\":\"20190711105006363114529432776998\",\"containerized\":false,\"name\":\"nginx-front\",\"hostname\":\"nginx-front\"},\"agent\":{\"version\":\"7.3.0\",\"type\":\"filebeat\",\"ephemeral_id\":\"af9d55ab-82b1-40db-a136-a6cf70489b49\",\"hostname\":\"nginx-front\",\"id\":\"2d89dfb9-ab4e-4078-b5b6-5fe4dc7f387a\"},\"cloud\":{\"instance\":{\"id\":\"i-wz9ic08951bdax2ujk2g\"},\"region\":\"cn-shenzhen\",\"availability_zone\":\"cn-shenzhen-e\",\"provider\":\"ecs\"}}"
    //val logDataURLString: String = java.net.URLDecoder.decode(logDataString,"UTF-8")
    println(logDataString)

    val jsonObject: JSONObject = JSON.parseObject(logDataString)
    val messageString: String = jsonObject.getString("message")
    //val decodeString: String = java.net.URLDecoder.decode(jsonMessage,"UTF-8")
    val jsonMessage: JSONObject = JSON.parseObject(messageString)
    val argString: String = jsonMessage.getString("args").replaceAll("params=","")
    val decodeString: String = java.net.URLDecoder.decode(argString,"UTF-8")
    println(decodeString)

  }
}
