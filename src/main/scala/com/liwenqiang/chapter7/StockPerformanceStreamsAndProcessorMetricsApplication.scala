package com.liwenqiang.chapter7

import com.liwenqiang.clients.producer.MockDataProducer
import com.liwenqiang.interceptors.StockTransactionConsumerInterceptor
import com.liwenqiang.transformer.supplier.StockPerformanceMetricsTransformerSupplier
import com.liwenqiang.util.model.{StockPerformance, StockTransaction}
import com.liwenqiang.util.serde.StreamsSerdes
import org.apache.kafka.common.{Metric, MetricName}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, Produced}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.{KeyValueBytesStoreSupplier, KeyValueStore, StoreBuilder, Stores}

import java.util.Properties

object StockPerformanceStreamsAndProcessorMetricsApplication {
  def main(args: Array[String]): Unit = {

    val stringSerde: Serde[String] = Serdes.stringSerde
    val stockPerformanceSerde: Serde[StockPerformance] = StreamsSerdes.StockPerformanceSerde()
    val stockTransactionSerde: Serde[StockTransaction] = StreamsSerdes.StockTransactionSerde()

    val builder = new StreamsBuilder()
    val stockStateStore = "stock-performance-store"
    val differentialThreshold = 0.05

    val storeSupplier: KeyValueBytesStoreSupplier = Stores.lruMap(stockStateStore, 100)
    val storeBuilder: StoreBuilder[KeyValueStore[String, StockPerformance]] = Stores.keyValueStoreBuilder(storeSupplier, stringSerde, stockPerformanceSerde)

    builder.addStateStore(storeBuilder)

    builder.stream("stock-transactions")(Consumed.`with`(stringSerde,stockTransactionSerde))
      .transform(new StockPerformanceMetricsTransformerSupplier(stockStateStore,differentialThreshold),stockStateStore)
      .peek((k: String, v: StockPerformance)=>println(s"[stock-performance] key: $k value: $v"))
      .to("stock-performance")(Produced.`with`(stringSerde,stockPerformanceSerde))

    val kafkaStreams = new KafkaStreams(builder.build(), getProperties)
    MockDataProducer.produceStockTransactionsWithKeyFunction(50,50,25,(v:StockTransaction)=>v.getSymbol)
    println("Stock Analysis KStream/Process API Metrics App Started")
    kafkaStreams.cleanUp()
    kafkaStreams.start()

    Thread.sleep(70000)

    println("Shutting down the Stock KStream/Process API Analysis Metrics  App now")

    kafkaStreams.metrics().entrySet().forEach(v=>{
      val metric: Metric = v.getValue
      val metricName: MetricName = v.getKey
      if (!metric.metricValue().equals(0.0) && !metric.metricValue().equals(Double.NegativeInfinity)) {
        print(s"MetricName ${metricName.name()}")
        println(s"= ${metric.metricValue()}")
      }

    })

    kafkaStreams.close()
    MockDataProducer.shutdown()

  }

  private val getProperties:Properties = {
    import org.apache.kafka.clients.consumer.ConsumerConfig
    import org.apache.kafka.streams.StreamsConfig
    import org.apache.kafka.streams.processor.WallclockTimestampExtractor
    import java.util.Collections
    val props = new Properties
    props.put(StreamsConfig.CLIENT_ID_CONFIG, "ks-stats-stock-analysis-client")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "ks-stats-stock-analysis-group")
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ks-stats-stock-analysis-appid")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1)
    props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass.getName)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass.getName)
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[WallclockTimestampExtractor])
    props.put(StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG), Collections.singletonList(classOf[StockTransactionConsumerInterceptor]))
    props
  }
}
