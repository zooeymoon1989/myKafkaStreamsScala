package com.liwenqiang

import com.liwenqiang.util.model.Purchase
import com.liwenqiang.util.serde.StreamsSerdes
import com.liwenqiang.util.serializer.{JsonDeserializer, JsonSerializer}

import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.{KeyValue, StreamsConfig}
import org.apache.kafka.streams.kstream.{KeyValueMapper, Predicate}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Consumed
import org.apache.kafka.streams.scala.serialization.Serdes.stringSerde
import com.liwenqiang.util.serde.StreamsSerdes.{PurchasePatternSerde, PurchaseSerde}


object KafkaStreamsJoinsApp {
  def main(args: Array[String]): Unit = {
    val streamsConfig = new StreamsConfig(getProperties)
    val builder = new StreamsBuilder()
    val purchaseSerde: Serde[Purchase] = StreamsSerdes.PurchaseSerde
    val stringSerde: Serde[String] = Serdes.String()

    val custIdCCMasking:KeyValueMapper[String,Purchase,KeyValue[String,Purchase]] = (k:String,v:Purchase)=>{
      val masked: Purchase = Purchase.builder(v).maskCreditCard().build()
      new KeyValue[String,Purchase](masked.getCustomerId,masked)
    }
    val isCoffee:Predicate[String,Purchase] = {
      (key:String,purchase:Purchase) => {
        purchase.getDepartment.equalsIgnoreCase("coffee")
      }
    }
    val isElectronics:Predicate[String,Purchase] = {
      (key:String,purchase:Purchase) => {
        purchase.getDepartment.equalsIgnoreCase("electronics")
      }
    }
    import org.apache.kafka.streams.scala.ImplicitConversions._
    builder.stream("transactions",Consumed.`with`[String,Purchase](stringSerde,purchaseSerde))(consumedFromSerde)

  }



  private def getProperties = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "join_driver_application")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "join_driver_group")
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "join_driver_client")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1")
    props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1)
//    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TransactionTimestampExtractor.get)
    props
  }

}
