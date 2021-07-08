package com.liwenqiang.chapter5

import com.liwenqiang.clients.producer.MockDataProducer
import com.liwenqiang.collectors.{FixedSizePriorityQueue, StockTransactionCollector}
import com.liwenqiang.util.model.{ShareVolume, StockTransaction}
import com.liwenqiang.util.serde.StreamsSerdes
import org.apache.kafka.streams.Topology.AutoOffsetReset
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream.{Consumed, KTable, Materialized}
import org.apache.kafka.streams.scala.serialization.Serdes

import java.util.Comparator

class AggregationsAndReducingExample {
  def main(args: Array[String]): Unit = {
    val builder = new StreamsBuilder()
    val shareVolume: KTable[String, ShareVolume] = builder.stream(MockDataProducer.STOCK_TRANSACTIONS_TOPIC)(Consumed.`with`(AutoOffsetReset.EARLIEST)(Serdes.stringSerde, StreamsSerdes.StockTransactionSerde()))
      .mapValues((st: StockTransaction) => ShareVolume.newBuilder(st).build())
      .groupBy((k: String, v: ShareVolume) => v.getSymbol)(Grouped.`with`(Serdes.stringSerde, StreamsSerdes.ShareVolumeSerde()))
      .reduce((s1: ShareVolume, s2: ShareVolume) => ShareVolume.sum(s1, s2))(Materialized.`with`[String, ShareVolume, ByteArrayKeyValueStore](Serdes.stringSerde, StreamsSerdes.ShareVolumeSerde()))
    val comparator: Comparator[ShareVolume] = (s1: ShareVolume, s2: ShareVolume) => s1.getShares - s2.getShares
    val fixedQueue = new FixedSizePriorityQueue(comparator, 5)
    val g = Grouped.`with`(Serdes.stringSerde, StreamsSerdes.ShareVolumeSerde())
    shareVolume.groupBy(
      (k: String, v: ShareVolume) => (v.getIndustry, v)
    )(Grouped.`with`(Serdes.stringSerde, StreamsSerdes.ShareVolumeSerde()))
      .aggregate(()=>fixedQueue)((k,v,agg:StockTransactionCollector)=>agg.add(v),(k,v,agg)=>agg.remove(v))(Materialized.`with`(Serdes.stringSerde,StreamsSerdes.ShareVolumeSerde()))

  }
}
