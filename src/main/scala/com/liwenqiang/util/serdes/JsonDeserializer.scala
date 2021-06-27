package com.liwenqiang.util.serdes

import com.google.gson.Gson
import org.apache.kafka.common.serialization.Deserializer

class JsonDeserializer[Any](deserialized:Class[Any]) extends Deserializer[Any]{

  private val gson:Gson = new Gson()
  private val deserializedClass:Class[Any] = deserialized

  override def deserialize(topic: String, data: Array[Byte]): Any ={
    if (data == null) {
      return data.asInstanceOf[Any]
    }
    gson.fromJson(new String(data),deserializedClass)
  }

  override def close(): Unit = {

  }
}
