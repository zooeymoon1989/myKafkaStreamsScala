package com.liwenqiang.chapter6

import com.liwenqiang.clients.producer.MockDataProducer
import com.liwenqiang.config.initConfig.InitGetProperties
import com.liwenqiang.processors.{CogroupingProcessor, KStreamPrinter}
import com.liwenqiang.processors.supplier.{ClickEventProcessorSupplier, CogroupingProcessorSupplier, KStreamPrinterSupplier, StockTransactionProcessorSupplier}
import com.liwenqiang.util.collection.Tuple
import com.liwenqiang.util.model.{ClickEvent, StockTransaction}
import com.liwenqiang.util.serde.StreamsSerdes
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.{KafkaStreams, Topology}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.{KeyValueBytesStoreSupplier, KeyValueStore, StoreBuilder, Stores}

import java.util

object CoGroupingApplication {
  def main(args: Array[String]): Unit = {

    val stringSerde: Serde[String] = Serdes.stringSerde
    val stringSerializer: Serializer[String] = stringSerde.serializer()
    val stringDeserializer: Deserializer[String] = stringSerde.deserializer()
    val eventPerformanceTuple: Serde[Tuple[util.List[ClickEvent], util.List[StockTransaction]]] = StreamsSerdes.EventTransactionTupleSerde()
    val tupleSerializer: Serializer[Tuple[util.List[ClickEvent], util.List[StockTransaction]]] = eventPerformanceTuple.serializer()
    val stockTransactionSerde: Serde[StockTransaction] = StreamsSerdes.StockTransactionSerde()
    val stockTransactionDeserializer: Deserializer[StockTransaction] = stockTransactionSerde.deserializer()

    val clickEventSerde: Serde[ClickEvent] = StreamsSerdes.ClickEventSerde()
    val clickEventDeserializer: Deserializer[ClickEvent] = clickEventSerde.deserializer()

    val topology = new Topology
//    val changeLogConfigs: util.HashMap[String,String] = new util.HashMap()
//    changeLogConfigs.put("retentions.ms","12000")
//    changeLogConfigs.put("cleanup.policy","compact.delete")

    val storeSupplier: KeyValueBytesStoreSupplier = Stores.persistentKeyValueStore(CogroupingProcessor.TUPLE_STORE_NAME)
    val storeBuilder: StoreBuilder[KeyValueStore[String, Tuple[util.List[ClickEvent], util.List[StockTransaction]]]] = Stores.keyValueStoreBuilder(
      storeSupplier,
      Serdes.stringSerde,
      eventPerformanceTuple
    )

    topology.addSource(
      "Txn-source",
      stringDeserializer,
      stockTransactionDeserializer,
      "stock-transactions"
    ).addSource(
      "Events-Source",
      stringDeserializer,
      clickEventDeserializer,
      "events"
    ).addProcessor(
      "Txn-processor",
      new StockTransactionProcessorSupplier,
      "Txn-source"
    ).addProcessor(
      "Events-Processor",
      new ClickEventProcessorSupplier,
      "Events-Source"
    ).addProcessor(
      "CoGrouping-Processor",
      new CogroupingProcessorSupplier,
      "Txn-processor","Events-Processor"
    ).addStateStore(
      storeBuilder,
      "CoGrouping-Processor"
    ).addSink(
      "Tuple-sink",
      "cogrouped-results",
      stringSerializer,
      tupleSerializer,
      "CoGrouping-Processor"
    )

    topology.addProcessor("Print",new KStreamPrinterSupplier,"CoGrouping-Processor")


    MockDataProducer.produceStockTransactionsAndDayTradingClickEvents(50,100,100,(v:StockTransaction)=>v.getSymbol)
    val kafkaStreams = new KafkaStreams(topology, new InitGetProperties("CoGroupingApplicationExample").GetProperties)
    println("Co-Grouping App Started")
    kafkaStreams.cleanUp()
    kafkaStreams.start()
    Thread.sleep(70000)
    println("Shutting down the Co-Grouping App now")
    kafkaStreams.close()
    MockDataProducer.shutdown()
  }
}
