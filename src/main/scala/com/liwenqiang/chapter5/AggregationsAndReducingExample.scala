package com.liwenqiang.chapter5

import com.liwenqiang.clients.producer.MockDataProducer
import com.liwenqiang.collectors.{FixedSizePriorityQueue, StockTransactionCollector}
import com.liwenqiang.util.model.{ShareVolume, StockTransaction}
import com.liwenqiang.util.serde.StreamsSerdes
import org.apache.kafka.streams.Topology.AutoOffsetReset
import org.apache.kafka.streams.kstream.{Grouped, ValueMapper}
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream.{Consumed, KTable, Materialized, Produced}
import org.apache.kafka.streams.scala.serialization.Serdes

import java.util
import java.util.Comparator
import java.text.NumberFormat

object AggregationsAndReducingExample {

  def main(args: Array[String]): Unit = {
    val builder = new StreamsBuilder()
    val shareVolume: KTable[String, ShareVolume] = builder.stream(MockDataProducer.STOCK_TRANSACTIONS_TOPIC)(Consumed.`with`(AutoOffsetReset.EARLIEST)(Serdes.stringSerde, StreamsSerdes.StockTransactionSerde()))
      .mapValues((st: StockTransaction) => ShareVolume.newBuilder(st).build())
      .groupBy((k: String, v: ShareVolume) => v.getSymbol)(Grouped.`with`(Serdes.stringSerde, StreamsSerdes.ShareVolumeSerde()))
      .reduce((s1: ShareVolume, s2: ShareVolume) => ShareVolume.sum(s1, s2))(Materialized.`with`[String, ShareVolume, ByteArrayKeyValueStore](Serdes.stringSerde, StreamsSerdes.ShareVolumeSerde()))
    val comparator: Comparator[ShareVolume] = (s1: ShareVolume, s2: ShareVolume) => s1.getShares - s2.getShares
    val fixedQueue = new FixedSizePriorityQueue(comparator, 5)
    //    val g = Grouped.`with`(Serdes.stringSerde, StreamsSerdes.ShareVolumeSerde())
    shareVolume.groupBy(
      (k: String, v: ShareVolume) => (v.getIndustry, v)
    )(Grouped.`with`(Serdes.stringSerde, StreamsSerdes.ShareVolumeSerde()))
      .aggregate(fixedQueue)((k: String, v: ShareVolume, agg: FixedSizePriorityQueue[ShareVolume]) => agg.add(v), (k: String, v: ShareVolume, agg: FixedSizePriorityQueue[ShareVolume]) => agg.remove(v))(Materialized.`with`(Serdes.stringSerde, StreamsSerdes.ShareVolumeSerde()))
      .mapValues((v: FixedSizePriorityQueue[ShareVolume]) => {
        val builder = new StringBuilder
        val iterator: util.Iterator[ShareVolume] = v.iterator()
        var counter = 1
        val numberFormat: NumberFormat = NumberFormat.getInstance()
        while (iterator.hasNext) {
          val stockVolume: ShareVolume = iterator.next()
          if (stockVolume != null) {
            counter = counter + 1
            builder.append(counter).append(")").append(stockVolume.getSymbol)
              .append(":").append(numberFormat.format(stockVolume.getShares)).append(" ")
          }
        }
        builder
      })
      .toStream.peek((k,v)=>println(s"Stock volume by industry $k $v"))
      .to("stock-volume-by-company")(Produced.`with`(Serdes.stringSerde,Serdes.stringSerde))
  }
}
