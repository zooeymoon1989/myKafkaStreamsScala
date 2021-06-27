package com.liwenqiang.util.serdes

import com.google.gson.Gson
import org.apache.kafka.common.serialization.Serializer

import java.nio.charset.Charset
import java.util

class JsonSerializer[Any] extends Serializer[Any]{

  private val gson:Gson = new Gson()

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {

  }

  override def serialize(topic: String, data: Any): Array[Byte] = {
    gson.toJson(data).getBytes(Charset.forName("UTF-8"))
  }

  override def close(): Unit = {

  }

}
