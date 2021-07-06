package com.liwenqiang.chapter5

import com.liwenqiang.clients.producer.MockDataProducer
import com.liwenqiang.config.initConfig.InitGetProperties
import com.liwenqiang.util.model.StockTickerData
import com.liwenqiang.util.serde.StreamsSerdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.scala.kstream.Consumed
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.{StreamsBuilder, kstream}
import org.slf4j.{Logger, LoggerFactory}


object KStreamVsKTableExample {
  val LOG: Logger = LoggerFactory.getLogger(KStreamVsKTableExample.getClass.getName)
  def main(args: Array[String]): Unit = {
    val builder = new StreamsBuilder()
    val consumed: Consumed[String, StockTickerData] =Consumed.`with`[String,StockTickerData](Serdes.stringSerde,StreamsSerdes.StockTickerSerde())
    val stockTickerTable: kstream.KTable[String, StockTickerData] = builder.table(MockDataProducer.STOCK_TICKER_TABLE_TOPIC)(consumed)
    val stockTickerStream: kstream.KStream[String, StockTickerData] = builder.stream(MockDataProducer.STOCK_TICKER_STREAM_TOPIC)(consumed)
    stockTickerTable.toStream.print(Printed.toSysOut[String,StockTickerData].withLabel("Stocks-KTable"))
    stockTickerStream.print(Printed.toSysOut[String,StockTickerData].withLabel("Stocks-KStream"))

    val numberCompanies = 3
    val iterations = 3

    MockDataProducer.produceStockTickerData(numberCompanies,iterations)

    val kafkaStreams = new KafkaStreams(builder.build(), new InitGetProperties("KStreamVSKTable_app").GetProperties)
    LOG.info("KTable vs KStream output started")
    kafkaStreams.cleanUp()
    kafkaStreams.start()
    Thread.sleep(15000)
    LOG.info("Shutting down KTable vs KStream Application now")
    kafkaStreams.close()
    MockDataProducer.shutdown()

  }
}
