package com.liwenqiang.chapter6

import com.liwenqiang.clients.producer.MockDataProducer
import com.liwenqiang.config.initConfig.InitGetProperties
import com.liwenqiang.processors.StockPerformanceProcessor
import com.liwenqiang.processors.supplier.{KStreamPrinterSupplier, StockPerformanceProcessorSupplier}
import com.liwenqiang.util.model.{StockPerformance, StockTransaction}
import com.liwenqiang.util.serde.StreamsSerdes
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.{KafkaStreams, Topology}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.{KeyValueBytesStoreSupplier, KeyValueStore, StoreBuilder, Stores}

object StockPerformanceApplication {
  def main(args: Array[String]): Unit = {

    val topology = new Topology
    val stocksStateStore = "stock-performance-store"
    val differentialThreshold = 0.02

    val stringSerde: Serde[String] = Serdes.stringSerde
    val stringSerializer: Serializer[String] = stringSerde.serializer()
    val stringDeserializer: Deserializer[String] = stringSerde.deserializer()
    val stockPerformanceSerde: Serde[StockPerformance] = StreamsSerdes.StockPerformanceSerde()
    val stockPerformanceSerializer: Serializer[StockPerformance] = stockPerformanceSerde.serializer()
    val stockTransactionSerde: Serde[StockTransaction] = StreamsSerdes.StockTransactionSerde()
    val stockTransactionDeserializer: Deserializer[StockTransaction] = stockTransactionSerde.deserializer()
    
    val storeSupplier: KeyValueBytesStoreSupplier = Stores.inMemoryKeyValueStore(stocksStateStore)
    val storeBuilder: StoreBuilder[KeyValueStore[String, StockPerformance]] = Stores.keyValueStoreBuilder(storeSupplier, stringSerde, stockPerformanceSerde)

    topology.addSource(
      "stocks-source",
      stringDeserializer,
      stockTransactionDeserializer,
      "stock-transactions"
    ).addProcessor(
      "stocks-processor",
      new StockPerformanceProcessorSupplier(stocksStateStore,differentialThreshold),
      "stocks-source"
    ).addStateStore(storeBuilder,"stocks-processor")
      .addSink(
        "stocks-sink",
        "stock-performance",
        stringSerializer,
        stockPerformanceSerializer,
        "stocks-processor"
      )

    topology.addProcessor("stocks-printer",new KStreamPrinterSupplier,"stocks-processor")

    val streams = new KafkaStreams(topology, new InitGetProperties("StockPerformanceApplicationExample").GetProperties)

    MockDataProducer.produceStockTransactionsWithKeyFunction(50,50,25,(v:StockTransaction)=>v.getSymbol)
    println("Stock Analysis App Started")
    streams.cleanUp()
    streams.start()
    Thread.sleep(70000)
    println("Shutting down the Stock Analysis App now")
    streams.close()
    MockDataProducer.shutdown()

  }
}
