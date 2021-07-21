package com.liwenqiang.chapter8

import com.liwenqiang.util.model.{Purchase, PurchasePattern, RewardAccumulator}
import com.liwenqiang.util.serde.StreamsSerdes
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{KafkaStreams, Topology}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, Produced}
import org.apache.kafka.streams.scala.serialization.Serdes

object ZMartTopology {
  def build(): Topology = {
    val purchaseSerde: Serde[Purchase] = StreamsSerdes.PurchaseSerde()
    val purchasePatternSerde: Serde[PurchasePattern] = StreamsSerdes.PurchasePatternSerde()
    val rewardAccumulatorSerde: Serde[RewardAccumulator] = StreamsSerdes.RewardAccumulatorSerde()
    val stringSerde: Serde[String] = Serdes.stringSerde

    val streamsBuilder = new StreamsBuilder()

    val purchaseKStream: KStream[String, Purchase] = streamsBuilder.stream("transactions")(Consumed.`with`(stringSerde, purchaseSerde))
      .mapValues((p: Purchase) => Purchase.builder(p).maskCreditCard().build())

    val patternKStream: KStream[String, PurchasePattern] = purchaseKStream.mapValues((p: Purchase) => PurchasePattern.builder(p).build())

    patternKStream.to("patterns")(Produced.`with`(stringSerde,purchasePatternSerde))

    val rewardsKStream: KStream[String, RewardAccumulator] = purchaseKStream.mapValues((p: Purchase) => RewardAccumulator.builder(p).build())


    rewardsKStream.to("rewards")(Produced.`with`(stringSerde,rewardAccumulatorSerde))

    purchaseKStream.to("purchases")(Produced.`with`(stringSerde,purchaseSerde))

    streamsBuilder.build()
  }


}
