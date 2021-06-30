package com.liwenqiang

import com.liwenqiang.partitioner.RewardsStreamPartitioner
import com.liwenqiang.supplier.PurchaseRewardTransformerSupplier
import com.liwenqiang.util.model.{Purchase, PurchasePattern, RewardAccumulator}
import com.liwenqiang.util.serde.StreamsSerdes
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.{Consumed, KStream, Predicate, Printed}
import org.apache.kafka.streams.scala.kstream.{Branched, Produced}
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import com.liwenqiang.util.serializer.{JsonDeserializer, JsonSerializer}
import org.apache.kafka.streams.state.{KeyValueBytesStoreSupplier, KeyValueStore, StoreBuilder, Stores}

import java.time.Duration
import java.util
import java.util.Properties

object ZMartKafkaStreamsApp {
  def main(args: Array[String]): Unit = {

    val props: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "ZMartKafkaStreamsApp")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      p
    }

    // 添加state store
    // 想streamBuilder里面添加state store
    val builder = new StreamsBuilder()
    //这个是state store的名称，以后直接调用这个string就OK了
    val rewardsStateStoreName = "rewardsPointsStore"
    val storeSupplier: KeyValueBytesStoreSupplier = Stores.inMemoryKeyValueStore(rewardsStateStoreName)
    val storeBuilder: StoreBuilder[KeyValueStore[String, Integer]] = Stores.keyValueStoreBuilder(storeSupplier, Serdes.StringSerde, Serdes.IntegerSerde)
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

    val purchaseSerde: Serde[Purchase] = Serdes.serdeFrom(purchaseJsonSerializer, purchaseJsonDeserializer)

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
    val transByCustomerStream: KStream[String, Purchase] = purchaseKStream.repartition()

    transByCustomerStream.transformValues(new PurchaseRewardTransformerSupplier("foobar"))
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

    purchaseKStream.split()
      .branch(isCoffee, Branched.withConsumer(_.to("coffee")(Produced.`with`(stringSerde, purchaseSerde))))
      .branch(isElectronics, Branched.withConsumer(_.to("electronics")(Produced.`with`(stringSerde, purchaseSerde))))


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
