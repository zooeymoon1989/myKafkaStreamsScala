package com.liwenqiang

import com.liwenqiang.util.model.{Purchase, PurchasePattern, RewardAccumulator}
import com.liwenqiang.util.serde.StreamsSerdes
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.{Consumed, ForeachAction, KStream, Printed}
import org.apache.kafka.streams.scala.kstream.Produced
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import com.liwenqiang.util.serializer.{JsonDeserializer, JsonSerializer}

import java.time.Duration
import java.util.Properties

object ZMartKafkaStreamsApp {
  def main(args: Array[String]): Unit = {

    val props:Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG,"ZMartKafkaStreamsApp")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
      p
    }

    val purchaseJsonSerializer:JsonSerializer[Purchase] = new JsonSerializer[Purchase]()

    val purchaseJsonDeserializer:JsonDeserializer[Purchase] = new JsonDeserializer[Purchase](classOf[Purchase])

    val purchaseSerde: Serde[Purchase] = Serdes.serdeFrom(purchaseJsonSerializer,purchaseJsonDeserializer)

    val stringSerde: Serde[String] = Serdes.String()

    val streamsBuilder:StreamsBuilder = new StreamsBuilder

    val purchaseKStream: KStream[String, Purchase] = streamsBuilder.stream("transactions", Consumed.`with`(stringSerde, purchaseSerde))
      .mapValues(p => Purchase.builder(p).maskCreditCard().build())
    val patternKStream: KStream[String, PurchasePattern] = purchaseKStream.mapValues(purchase => PurchasePattern.builder(purchase).build())

    patternKStream.to("patterns",Produced.`with`(stringSerde,StreamsSerdes.PurchasePatternSerde))

    val rewardsKStream: KStream[String, RewardAccumulator] = purchaseKStream.mapValues(purchase => RewardAccumulator.builder(purchase).build())

    rewardsKStream.to("rewards",Produced.`with`(stringSerde,StreamsSerdes.RewardAccumulatorSerde))

    purchaseKStream.to("purchases",Produced.`with`(stringSerde,StreamsSerdes.PurchaseSerde))

    val kafkaStreams = new KafkaStreams(streamsBuilder.build(),props)

    kafkaStreams.start()

    sys.ShutdownHookThread {
      kafkaStreams.close(Duration.ofSeconds(10))
    }
  }
}
