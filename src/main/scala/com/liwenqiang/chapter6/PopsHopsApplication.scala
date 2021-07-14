package com.liwenqiang.chapter6

import com.liwenqiang.processors.BeerPurchaseProcessor
import com.liwenqiang.processors.supplier.BeerPurchaseProcessorSupplier
import com.liwenqiang.util.Topics
import com.liwenqiang.util.model.BeerPurchase
import com.liwenqiang.util.serializer.{JsonDeserializer, JsonSerializer}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.Topology.AutoOffsetReset
import org.apache.kafka.streams.processor.UsePartitionTimeOnInvalidTimestamp
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.scala.serialization.Serdes

object PopsHopsApplication {

  def main(args: Array[String]): Unit = {

    val purchaseSourceNodeName = "beer-purchase-source"
    val purchaseProcessor = "purchase-processor"
    val domesticSalesSink = "domestic-beer-sales"
    val internationalSalesSink = "international-beer-sales"

    val stringSerde: Serde[String] = Serdes.stringSerde
    val stringSerializer: Serializer[String] = stringSerde.serializer()
    val stringDeserializer: Deserializer[String] = stringSerde.deserializer()
    val beerPurchaseSerializer = new JsonSerializer[BeerPurchase]()
    val beerPurchaseDeserializer = new JsonDeserializer[BeerPurchase]()

    val beerPurchaseProcessorSupplier = new BeerPurchaseProcessorSupplier(domesticSalesSink, internationalSalesSink)

    val topology = new Topology()
    topology.addSource(
      AutoOffsetReset.LATEST, // 设置偏移量
      purchaseSourceNodeName, // 设置节点名称
      new UsePartitionTimeOnInvalidTimestamp, // 设置使用的timestamp extractor
      stringDeserializer, // key的deserializer
      beerPurchaseDeserializer, // value的deserializer
      Topics.POPS_HOPS_PURCHASES.topicName() // 设置要从哪个topic中消费
    ).addProcessor(
      purchaseProcessor,
      beerPurchaseProcessorSupplier,
      purchaseSourceNodeName
    )



  }
}
