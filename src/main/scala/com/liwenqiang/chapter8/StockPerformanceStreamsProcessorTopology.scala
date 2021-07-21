package com.liwenqiang.chapter8

import com.liwenqiang.transformer.supplier.StockPerformanceMetricsTransformerSupplier
import org.apache.kafka.streams.scala.serialization.Serdes
import com.liwenqiang.util.model.StockPerformance
import com.liwenqiang.util.model.StockTransaction
import com.liwenqiang.util.serde.StreamsSerdes
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Consumed
import org.apache.kafka.streams.state.{KeyValueBytesStoreSupplier, KeyValueStore, StoreBuilder, Stores}

object StockPerformanceStreamsProcessorTopology {
  def build() = {

    val stringSerde: Serde[String] = Serdes.stringSerde
    val stockPerformanceSerde: Serde[StockPerformance] = StreamsSerdes.StockPerformanceSerde
    val stockTransactionSerde: Serde[StockTransaction] = StreamsSerdes.StockTransactionSerde

    val builder = new StreamsBuilder()

    val stocksStateStore = "stock-performance-store"

    val differentialThreshold = 0.02

    val storeStoreSupplier: KeyValueBytesStoreSupplier = Stores.lruMap(stocksStateStore, 100)

    val storeBuilder: StoreBuilder[KeyValueStore[String, StockPerformance]] = Stores.keyValueStoreBuilder(storeStoreSupplier, stringSerde, stockPerformanceSerde)

    builder.addStateStore(storeBuilder)

    builder.stream("stock-transactions")(Consumed.`with`(stringSerde,stockTransactionSerde))
      .transform(()->new StockPerformanceMetricsTransformerSupplier(stocksStateStore,differentialThreshold))

  }
}
