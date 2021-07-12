package com.liwenqiang.chapter5

import com.liwenqiang.clients.producer.MockDataProducer
import com.liwenqiang.util.model.{StockTransaction, TransactionSummary}
import com.liwenqiang.util.serde.StreamsSerdes
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology.AutoOffsetReset
import org.apache.kafka.streams.kstream.{Printed, SessionWindows, Windowed}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, KStream, KTable, Materialized}
import org.apache.kafka.streams.scala.serialization.Serdes

import java.time.Duration

object CountingWindowingAndKtableJoinExample {
  def main(args: Array[String]): Unit = {

    val stringSerde: Serde[String] = Serdes.stringSerde
    val transactionSerde: Serde[StockTransaction] = StreamsSerdes.StockTransactionSerde()
    val transactionKeySerde:Serde[TransactionSummary] = StreamsSerdes.TransactionSummarySerde()


    val twentySeconds: Long = 20
    val fifteenMinutes:Long =  15

    val builder = new StreamsBuilder()

    val customerTransactionCounts: KTable[Windowed[TransactionSummary], Long] = builder.stream(MockDataProducer.STOCK_TRANSACTIONS_TOPIC)(Consumed.`with`(stringSerde, transactionSerde).withOffsetResetPolicy(AutoOffsetReset.LATEST))
      .groupBy((nokey: String, transaction: StockTransaction) => TransactionSummary.from(transaction))(Grouped.`with`(transactionKeySerde, transactionSerde))
      .windowedBy(SessionWindows.`with`(Duration.ofSeconds(twentySeconds)).until(fifteenMinutes)).count()(Materialized.`with`(stringSerde, transactionKeySerde))

    customerTransactionCounts.toStream.print(Printed.toSysOut[Windowed[TransactionSummary],Long].withLabel("Customer Transactions Counts"))
  }
}
