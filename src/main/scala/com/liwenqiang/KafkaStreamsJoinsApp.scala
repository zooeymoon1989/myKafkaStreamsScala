package com.liwenqiang

import com.liwenqiang.joiner.PurchaseJoiner
import com.liwenqiang.util.model.{CorrelatedPurchase, Purchase}
import com.liwenqiang.util.serde.StreamsSerdes
import com.liwenqiang.timestamp_extractor.TransactionTimestampExtractor

import java.util.{Properties, UUID}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsBuilder, StreamsConfig, kstream}
import org.apache.kafka.streams.kstream.{BranchedKStream, JoinWindows, KStream, KeyValueMapper, Named, Predicate, Printed}
import org.apache.kafka.streams.scala.kstream.{Branched, Consumed, KTable, Produced, StreamJoined}

import java.time.Duration
import java.util


object KafkaStreamsJoinsApp {
  def main(args: Array[String]): Unit = {
    val streamsConfig = new StreamsConfig(getProperties)
    val builder: StreamsBuilder = new StreamsBuilder()
    val purchaseSerde: Serde[Purchase] = StreamsSerdes.PurchaseSerde
    val stringSerde: Serde[String] = Serdes.String()
    val consumed: Consumed[String, Purchase] = Consumed.`with`[String, Purchase](stringSerde, purchaseSerde).withTimestampExtractor(TransactionTimestampExtractor)
    val isCoffee: Predicate[String, Purchase] = (_: String, v: Purchase) => {
      v.getDepartment.equalsIgnoreCase("coffee")
    }
    val isElectronics: Predicate[String, Purchase] = (_: String, v: Purchase) => {
      v.getDepartment.equalsIgnoreCase("electronics")
    }
    val transactionStream: KStream[String, Purchase] = builder.stream("transactions", consumed)
      .map((k: String, v: Purchase) => {
        val masked: Purchase = Purchase.builder(v).maskCreditCard().build()
        new KeyValue[String, Purchase](masked.getCustomerId, masked)
      })

    val branchStream: util.Map[String, KStream[String, Purchase]] = transactionStream.selectKey(new KeyValueMapper[String, Purchase, String] {
      override def apply(key: String, value: Purchase): String = {
        value.getCustomerId
      }
    }).split(Named.as("foo-")) //添加别名
      .branch(isCoffee, Branched.as("coffee"))
      .branch(isElectronics, Branched.as("electronics"))
      .noDefaultBranch()

    val coffeeStream: kstream.KStream[String, Purchase] = branchStream.get("foo-coffee")
    val electronicsStream: kstream.KStream[String, Purchase] = branchStream.get("foo-electronics")

    val purchaseJoiner = new PurchaseJoiner

    val twentyMinuteWindow: JoinWindows = JoinWindows.of(Duration.ofSeconds(5))

    val joinedStream: kstream.KStream[String, CorrelatedPurchase] = coffeeStream.outerJoin(
      electronicsStream
      , purchaseJoiner
      , twentyMinuteWindow,
      StreamJoined.`with`[String, Purchase, Purchase](stringSerde, purchaseSerde, purchaseSerde)
    )

    joinedStream.print(Printed.toSysOut.asInstanceOf[Printed[String, CorrelatedPurchase]].withLabel("joined KStream"))

    val kafkaStreams = new KafkaStreams(builder.build(), getProperties)

    kafkaStreams.start()
    sys.ShutdownHookThread {
      kafkaStreams.close(Duration.ofSeconds(10))
    }
  }


  private def getProperties = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "join_driver_group")
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "join_driver_client")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1")
    props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1)
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TransactionTimestampExtractor)
    props
  }

}
