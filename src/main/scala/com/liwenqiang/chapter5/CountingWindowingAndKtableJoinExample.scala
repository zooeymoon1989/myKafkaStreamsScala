package com.liwenqiang.chapter5

import com.liwenqiang.clients.producer.MockDataProducer
import com.liwenqiang.util.model.{StockTransaction, TransactionSummary}
import com.liwenqiang.util.serde.StreamsSerdes
import com.liwenqiang.util.serde.StreamsSerdes.TransactionSummarySerde
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology.AutoOffsetReset
import org.apache.kafka.streams.kstream.{Printed, SessionWindows, TimeWindows, ValueJoiner, Windowed}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, Joined, KStream, KTable, Materialized}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes.{longSerde, stringSerde}

import java.time.Duration

object CountingWindowingAndKtableJoinExample {
  def main(args: Array[String]): Unit = {

    val stringSerde: Serde[String] = Serdes.stringSerde
    val transactionSerde: Serde[StockTransaction] = StreamsSerdes.StockTransactionSerde()
    val transactionSummarySerde:Serde[TransactionSummary] = StreamsSerdes.TransactionSummarySerde()

    val twentySeconds: Long = 20
    val fifteenMinutes:Long =  15

    val builder = new StreamsBuilder()

    val customerTransactionCounts: KTable[Windowed[TransactionSummary], Long] = builder.stream(MockDataProducer.STOCK_TRANSACTIONS_TOPIC)(Consumed.`with`(stringSerde, transactionSerde).withOffsetResetPolicy(AutoOffsetReset.LATEST))
      .groupBy((nokey: String, transaction: StockTransaction) => TransactionSummary.from(transaction))(Grouped.`with`(transactionSummarySerde, transactionSerde))
      // session window
      .windowedBy(SessionWindows.`with`(Duration.ofSeconds(twentySeconds)).until(fifteenMinutes)).count()(Materialized.`with`(transactionSummarySerde,longSerde))

      // tumbling window
//      .windowedBy(TimeWindows.of(Duration.ofSeconds(twentySeconds))).count()(Materialized.`with`(stringSerde, transactionKeySerde))

      // sliding window
//      .windowedBy(TimeWindows.of(Duration.ofSeconds(twentySeconds)).advanceBy(Duration.ofSeconds(5)).until(fifteenMinutes)).count()(Materialized.`with`(stringSerde, transactionKeySerde))

    customerTransactionCounts.toStream.print(Printed.toSysOut[Windowed[TransactionSummary],Long].withLabel("Customer Transactions Counts"))

    val countStream: KStream[String, TransactionSummary] = customerTransactionCounts.toStream.map((window: Windowed[TransactionSummary], count: Long) => {
      val transactionSummary: TransactionSummary = window.key()
      val newKey: String = transactionSummary.getIndustry
      transactionSummary.setSummaryCount(count)
      (newKey, transactionSummary)
    })

    val financialNews: KTable[String, Long] = builder.table("financial-news")(Consumed.`with`(stringSerde,longSerde))

    val joined: KStream[String, String] = countStream.leftJoin(financialNews)((t: TransactionSummary, s: Long) => {
      s"${t.getSummaryCount} shares purchased ${t.getSummaryCount} related news $s"
    })(Joined.`with`(stringSerde, StreamsSerdes.TransactionSummarySerde, longSerde))


    joined.print(Printed.toSysOut[String,String].withLabel("Transactions and News"))

  }
}
