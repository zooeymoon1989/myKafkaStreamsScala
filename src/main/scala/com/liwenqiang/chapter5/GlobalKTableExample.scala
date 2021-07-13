package com.liwenqiang.chapter5

import com.liwenqiang.clients.producer.MockDataProducer
import com.liwenqiang.config.initConfig.InitGetProperties
import com.liwenqiang.util.Topics
import com.liwenqiang.util.datagen.{CustomDateGenerator, DataGenerator}
import com.liwenqiang.util.model.{StockTransaction, TransactionSummary}
import com.liwenqiang.util.serde.StreamsSerdes
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.Topology.AutoOffsetReset
import org.apache.kafka.streams.kstream.{GlobalKTable, Printed, SessionWindows, Windowed}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, KStream, Materialized}
import org.apache.kafka.streams.scala.serialization.Serdes

import java.time.Duration

object GlobalKTableExample {
  def main(args: Array[String]): Unit = {
    val stringSerde: Serde[String] = Serdes.stringSerde
    val transactionSerde: Serde[StockTransaction] = StreamsSerdes.StockTransactionSerde()
    val transactionSummarySerde: Serde[TransactionSummary] = StreamsSerdes.TransactionSummarySerde()

    val builder = new StreamsBuilder()
    val countStream: KStream[String, TransactionSummary] = builder.stream(MockDataProducer.STOCK_TRANSACTIONS_TOPIC)(Consumed.`with`(stringSerde, transactionSerde).withOffsetResetPolicy(AutoOffsetReset.LATEST))
      .groupBy((nokey: String, transaction: StockTransaction) => {
        TransactionSummary.from(transaction)
      })(Grouped.`with`(transactionSummarySerde, transactionSerde))
      .windowedBy(SessionWindows.`with`(Duration.ofSeconds(20)))
      .count()(Materialized.`with`(transactionSummarySerde,Serdes.longSerde))
      .toStream.map((k: Windowed[TransactionSummary], v: Long) => {
      val transactionSummary: TransactionSummary = k.key()
      val newKey: String = transactionSummary.getIndustry
      transactionSummary.setSummaryCount(v)
      (newKey, transactionSummary)
    })

    countStream.print(Printed.toSysOut[String, TransactionSummary].withLabel("foo-bar"))

    val publicCompanies: GlobalKTable[String, String] = builder.globalTable(Topics.COMPANIES.topicName())(Consumed.`with`(stringSerde, stringSerde))
    val clients: GlobalKTable[String, String] = builder.globalTable(Topics.CLIENTS.topicName())(Consumed.`with`(stringSerde, stringSerde))

    countStream.leftJoin(publicCompanies)((k: String, v: TransactionSummary)=>{

    })

    val kafkaStreams = new KafkaStreams(builder.build(), new InitGetProperties("GlobalKTableExample").GetProperties)
    kafkaStreams.cleanUp()

    val dateGenerator: CustomDateGenerator = CustomDateGenerator.withTimestampsIncreasingBy(Duration.ofMillis(750))
    DataGenerator.setTimestampGenerator(DataGenerator.getTimestampGenerator)

    MockDataProducer.produceStockTransactions(2,5,3,true)
    println("Starting GlobalKTable Example")
    kafkaStreams.cleanUp()
    kafkaStreams.start()
    Thread.sleep(65000)
    println("Shutting down the GlobalKTable Example Application now")
    kafkaStreams.close()
    MockDataProducer.shutdown()

  }
}
