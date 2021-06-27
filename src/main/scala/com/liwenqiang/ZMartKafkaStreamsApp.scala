package com.liwenqiang

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{Consumed, KStream}
import org.apache.kafka.streams.scala.kstream.Produced
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.apache.kafka.streams.scala.serialization.Serdes

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

    val purchaseJsonDeserializer:JsonDeserializer[Purchase] = new JsonDeserializer[Purchase]()

    val purchaseSerde: Serdes[Purchase] = Serdes.fromFn(purchaseJsonSerializer, purchaseJsonDeserializer)

    val stringSerde: Serde[String] = Serdes.stringSerde

    val streamsBuilder:StreamsBuilder = new StreamsBuilder

    val purchaseKStream: KStream[String, Purchase] = streamsBuilder.stream("transactions", Consumed.`with`(stringSerde, purchaseSerde))
      .mapValues(p => Purchase.builder(p).maskcreditCard().build())

    val patternKStream: KStream[String, PurchasePattern] = purchaseKStream.mapValues(purchase => PurchasePattern.builder(purchase))

    patternKStream.to("patterns",Produced.`with`(stringSerde,purchasePatternSerde))

    val rewardsKStream: KStream[String, RewardAccumulator] = purchaseKStream.mapValues(purchase => RewardAccumulator.builder(purchase).build())

    rewardsKStream.to("rewards",Produced.`with`(stringSerde,rewardAccumulatorSerde))

    rewardsKStream.to("purchases",Produced.`with`(stringSerde,purchaseSerde))

    val kafkaStreams = new KafkaStreams(streamsBuilder.build(),props)

    kafkaStreams.start()

    sys.ShutdownHookThread {
      kafkaStreams.close(Duration.ofSeconds(10))
    }
  }
}
