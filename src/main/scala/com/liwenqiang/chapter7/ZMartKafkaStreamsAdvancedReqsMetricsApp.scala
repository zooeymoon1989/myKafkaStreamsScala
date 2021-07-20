package com.liwenqiang.chapter7

import com.liwenqiang.clients.producer.MockDataProducer
import com.liwenqiang.interceptors.ZMartProducerInterceptor
import com.liwenqiang.util.datagen.DataGenerator
import com.liwenqiang.util.model.{Purchase, PurchasePattern, RewardAccumulator}
import com.liwenqiang.util.serde.StreamsSerdes
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Branched, Consumed, KStream, Produced}
import org.apache.kafka.streams.scala.serialization.Serdes

import java.util.concurrent.CountDownLatch
import java.util.{Collections, Properties}

object ZMartKafkaStreamsAdvancedReqsMetricsApp {
  def main(args: Array[String]): Unit = {


    val purchaseSerde: Serde[Purchase] = StreamsSerdes.PurchaseSerde()
    val purchasePatternSerde: Serde[PurchasePattern] = StreamsSerdes.PurchasePatternSerde()
    val rewardAccumulatorSerde: Serde[RewardAccumulator] = StreamsSerdes.RewardAccumulatorSerde()
    val stringSerde: Serde[String] = Serdes.stringSerde


    val streamsBuilder = new StreamsBuilder()
    val purchaseKStream: KStream[String, Purchase] = streamsBuilder.stream("transaction")(Consumed.`with`(stringSerde, purchaseSerde))
      .mapValues((p: Purchase) => Purchase.builder(p).maskCreditCard().build())

    val patternKStream: KStream[String, PurchasePattern] = purchaseKStream.mapValues((p: Purchase) => PurchasePattern.builder(p).build())

    patternKStream.to("patterns")(Produced.`with`(stringSerde,purchasePatternSerde))

    val rewardsKStream: KStream[String, RewardAccumulator] = purchaseKStream.mapValues((p: Purchase) => RewardAccumulator.builder(p).build())

    rewardsKStream.to("reward")(Produced.`with`(stringSerde,rewardAccumulatorSerde))

    val filteredKStream: KStream[Long, Purchase] = purchaseKStream.filter((k: String, v: Purchase) => v.getPrice > 5.00).selectKey((k: String, v: Purchase) => v.getPurchaseDate.getTime)

    filteredKStream.to("purchases")(Produced.`with`(Serdes.longSerde,purchaseSerde))

    purchaseKStream
      .split()
      .branch((k: String, v: Purchase)=>v.getDepartment.equalsIgnoreCase("coffee"),Branched.as("coffee"))
      .branch((k: String, v: Purchase)=>v.getDepartment.equalsIgnoreCase("electronics"),Branched.as("electronics"))
      .noDefaultBranch()

    purchaseKStream.filter((k: String, v: Purchase)=>v.getEmployeeId.eq("000000")).foreach((k:String, v:Purchase)=>{})


    val topology: Topology = streamsBuilder.build()


    val kafkaStreams = new KafkaStreams(topology, props)

    kafkaStreams.setStateListener((newState: KafkaStreams.State, oldState: KafkaStreams.State) => {
      if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
        println("Application has gone from REBALANCING to RUNNING ")
        println(s"Topology Layout ${streamsBuilder.build().describe()}")
      }

      if (newState == KafkaStreams.State.REBALANCING) {
        println("Application is entering REBALANCING phase")
      }
    })

    println("ZMart Advanced Requirements Metrics Application Started")

    kafkaStreams.cleanUp()
    val stopSignal = new CountDownLatch(1)

    Runtime.getRuntime.addShutdownHook(new Thread(
      ()=>{
        println("Shutting down the Kafka Streams Application now")
        kafkaStreams.close()
        MockDataProducer.shutdown()
        stopSignal.countDown()
      }
    ))

    MockDataProducer.producePurchaseData(DataGenerator.DEFAULT_NUM_PURCHASES,250,DataGenerator.NUMBER_UNIQUE_CUSTOMERS)
    kafkaStreams.start()
    stopSignal.await()
    println("All done now , good-bye")
  }
  
  private val props: Properties = {
    val ZMartKafkaStreamsAdvancedReqsMetricsAppConfig = new Properties()
    ZMartKafkaStreamsAdvancedReqsMetricsAppConfig.put(StreamsConfig.CLIENT_ID_CONFIG, "zmart-metrics-client-id")
    ZMartKafkaStreamsAdvancedReqsMetricsAppConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "zmart-metrics-group-id")
    ZMartKafkaStreamsAdvancedReqsMetricsAppConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "zmart-metrics-application-id")
    ZMartKafkaStreamsAdvancedReqsMetricsAppConfig.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG")
    ZMartKafkaStreamsAdvancedReqsMetricsAppConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    ZMartKafkaStreamsAdvancedReqsMetricsAppConfig.put(StreamsConfig.producerPrefix(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG), Collections.singletonList(classOf[ZMartProducerInterceptor]))
    ZMartKafkaStreamsAdvancedReqsMetricsAppConfig
  } 
}
