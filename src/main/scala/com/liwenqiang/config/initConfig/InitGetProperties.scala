package com.liwenqiang.config.initConfig

import com.liwenqiang.timestamp_extractor.TransactionTimestampExtractor
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.StreamsConfig

import java.util.Properties

class InitGetProperties(appName: String) {
  val GetProperties: Properties = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "join_driver_group")
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "join_driver_client")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1")
    props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1)
//    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TransactionTimestampExtractor.getClass)
    props
  }
}
