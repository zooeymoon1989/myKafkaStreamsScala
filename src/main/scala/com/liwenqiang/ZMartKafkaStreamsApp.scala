package com.liwenqiang

import com.liwenqiang.interceptors.ZMartProducerInterceptor
import com.liwenqiang.joiner.PurchaseJoiner
import com.liwenqiang.partitioner.RewardsStreamPartitioner
import com.liwenqiang.supplier.PurchaseRewardTransformerSupplier
import com.liwenqiang.util.model.{CorrelatedPurchase, Purchase, PurchasePattern, RewardAccumulator}
import com.liwenqiang.util.serde.StreamsSerdes
import com.liwenqiang.util.serializer.{JsonDeserializer, JsonSerializer}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.{Consumed, JoinWindows, KStream, KeyValueMapper, Predicate, Printed}
import org.apache.kafka.streams.scala.kstream.{Branched, Produced, StreamJoined}
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.apache.kafka.streams.state.{KeyValueBytesStoreSupplier, KeyValueStore, StoreBuilder, Stores}

import java.time.Duration
import java.util
import java.util.{Collections, Properties}

object ZMartKafkaStreamsApp {
  def main(args: Array[String]): Unit = {

    val props: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.CLIENT_ID_CONFIG, "zmart-metrics-client-id")
      p.put(ConsumerConfig.GROUP_ID_CONFIG, "zmart-metrics-group-id")
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "zmart-metrics-application-id")
      p.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      p.put(StreamsConfig.producerPrefix(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG), Collections.singletonList(classOf[ZMartProducerInterceptor]))
      p
    }

    // 添加state store
    // 想streamBuilder里面添加state store
    val builder = new StreamsBuilder()
    //这个是state store的名称，以后直接调用这个string就OK了
    val rewardsStateStoreName = "rewardsPointsStore"
    val storeSupplier: KeyValueBytesStoreSupplier = Stores.inMemoryKeyValueStore(rewardsStateStoreName)
    val storeBuilder: StoreBuilder[KeyValueStore[String, Integer]] = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Integer())
    // 在store builder中添加配置
    // 设置一个保存2天，10gb的日志配置
    val changeLogConfigs = new util.HashMap[String, String]()
    changeLogConfigs.put("retentions.ms","172800000")
    changeLogConfigs.put("retentions.bytes","")
    //清理日志方法
    changeLogConfigs.put("cleanup.policy", "compact,delete")
    storeBuilder.withLoggingEnabled(changeLogConfigs)
    builder.addStateStore(storeBuilder)

    val purchaseJsonSerializer: JsonSerializer[Purchase] = new JsonSerializer[Purchase]()

    val purchaseJsonDeserializer: JsonDeserializer[Purchase] = new JsonDeserializer[Purchase](classOf[Purchase])

    val purchaseSerde: Serde[Purchase] = StreamsSerdes.PurchaseSerde

    val stringSerde: Serde[String] = Serdes.String()

    val longSerde: Serde[Long] = Serdes.Long().asInstanceOf[Serde[scala.Long]]

    val streamsBuilder: StreamsBuilder = new StreamsBuilder

    val purchaseKStream: KStream[String, Purchase] = streamsBuilder.stream("transactions", Consumed.`with`(stringSerde, purchaseSerde))
      .mapValues(p => Purchase.builder(p).maskCreditCard().build())

    val filteredKStream: KStream[Long, Purchase] = purchaseKStream.filter(
      (key:String, purchase: Purchase) =>
        purchase.getPrice > 5.00
    ).selectKey(
      (key:String, purchase: Purchase) =>
        purchase.getPurchaseDate.getTime
    )

    val patternKStream: KStream[String, PurchasePattern] = purchaseKStream.mapValues(purchase => PurchasePattern.builder(purchase).build())
    patternKStream.print(Printed.toSysOut[String,PurchasePattern].withLabel("purchase"))
    patternKStream.to("patterns", Produced.`with`(stringSerde, StreamsSerdes.PurchasePatternSerde))

    val rewardsKStream: KStream[String, RewardAccumulator] = purchaseKStream.mapValues(purchase => RewardAccumulator.builder(purchase).build())
    //自定义分片
    val rewardsStreamPartitioner = new RewardsStreamPartitioner()
//    val transByCustomerStream: KStream[String, Purchase] = purchaseKStream.through("foobar",Produced.`with`(stringSerde,StreamsSerdes.PurchaseSerde))
    val transByCustomerStream: KStream[String, RewardAccumulator] = purchaseKStream
      .repartition().
      transformValues(new PurchaseRewardTransformerSupplier(rewardsStateStoreName))
    rewardsKStream.print(Printed.toSysOut[String,RewardAccumulator].withLabel("purchase"))
    rewardsKStream.to("rewards", Produced.`with`(stringSerde, StreamsSerdes.RewardAccumulatorSerde))

    filteredKStream.print(Printed.toSysOut[Long,Purchase].withLabel("purchase"))
    filteredKStream.to("purchases", Produced.`with`(longSerde, StreamsSerdes.PurchaseSerde))

    // predict
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

    val branchStream: util.Map[String, KStream[String, Purchase]] = purchaseKStream.selectKey(new KeyValueMapper[String,Purchase,String]{
      override def apply(key: String, value: Purchase): String = {
        value.getCustomerId
      }
    }).split()
      .branch(isCoffee, Branched.withConsumer(_.to("coffee")(Produced.`with`(Serdes.String(), purchaseSerde))))
      .branch(isElectronics, Branched.withConsumer(_.to("electronics")(Produced.`with`(stringSerde, purchaseSerde))))
      .noDefaultBranch()
    val coffeeStream: KStream[String, Purchase] = branchStream.get("coffee")
    val electronicsStream: KStream[String, Purchase] = branchStream.get("electronics")

    val purchaseJoiner = new PurchaseJoiner()

    val twentyMinuteWindow: JoinWindows = JoinWindows.of(Duration.ofMinutes(20))

    val joins: StreamJoined[String, Purchase, Purchase] = StreamJoined.`with`(stringSerde,purchaseSerde,purchaseSerde)
//    val a = new StreamJoined[String,Purchase,Purchase]
    val joinedKStream: KStream[String, CorrelatedPurchase] = coffeeStream.join(electronicsStream
      , purchaseJoiner
      , twentyMinuteWindow
      , joins
    )

    joinedKStream.print(Printed.toSysOut[String,CorrelatedPurchase].withLabel("joinedStream"))

    purchaseKStream.filter(
      (key:String,purchase:Purchase) => {
        purchase.getEmployeeId.equals("000000")
      }
    ).foreach((key:String,purchase:Purchase) => {})

    val kafkaStreams = new KafkaStreams(streamsBuilder.build(), props)

    kafkaStreams.start()
    sys.ShutdownHookThread {
      kafkaStreams.close(Duration.ofSeconds(10))
    }
  }
}
