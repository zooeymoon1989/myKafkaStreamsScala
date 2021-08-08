package com.liwenqiang.chapter9

import com.liwenqiang.clients.producer.MockDataProducer
import com.liwenqiang.interceptors.StockTransactionConsumerInterceptor
import com.liwenqiang.util.model.StockTransaction
import com.liwenqiang.util.serde.StreamsSerdes
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, KTable, Materialized, Produced}

import java.util.Collections
import java.util.Properties
import java.util.concurrent.CountDownLatch

object StockCountsStreamsConnectIntegrationApplication {
  def main(args: Array[String]): Unit = {

    val somePros: Properties = {
      val props = new Properties()
      props.put(StreamsConfig.CLIENT_ID_CONFIG, "ks-connect-stock-analysis-client")
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "ks-connect-stock-analysis-group")
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ks-connect-stock-analysis-appid")
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1)
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
      props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass.getName)
      props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass.getName)
      props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[WallclockTimestampExtractor])
      props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, classOf[DeserializerErrorHandler])
      props.put(StreamsConfig.consumerPrefix(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG), Collections.singletonList(classOf[StockTransactionConsumerInterceptor]))
      props
    }


    val stringSerde: Serde[String] = Serdes.stringSerde
    val stockTransactionSerde: Serde[StockTransaction] = StreamsSerdes.StockTransactionSerde()
    val longSerde: Serde[Long] = Serdes.longSerde

    val streamsBuilder = new StreamsBuilder()
    streamsBuilder.stream("dbTxnTRANSACTIONS")(Consumed.`with`(stringSerde, stockTransactionSerde))
      .peek((k: String, v: StockTransaction) => println(s"transactions from database key $k value $v"))
      .groupByKey(Grouped.`with`(stringSerde, stockTransactionSerde))
      .aggregate(0L)((symb: String, stockTxn: StockTransaction, numShares: Long) => numShares + stockTxn.getShares)(Materialized.`with`(stringSerde, longSerde))
      .toStream.peek((k: String, v: Long) => println(s"Aggregated stock sales for $k $v"))
      .to("stock-counts")(Produced.`with`(stringSerde, longSerde))

    val kafkaStreams = new KafkaStreams(streamsBuilder.build(), somePros)
    val doneSignal = new CountDownLatch(1)
    Runtime.getRuntime.addShutdownHook(new Thread(
      ()=>{
        println("Shutting down the Kafka Streams Application now")
        kafkaStreams.close()
        MockDataProducer.shutdown()
        doneSignal.countDown()
      }
    ))

    println("Stock Analysis KStream Connect App Started")
    kafkaStreams.cleanUp()
    kafkaStreams.start()
    doneSignal.await()
  }
}
