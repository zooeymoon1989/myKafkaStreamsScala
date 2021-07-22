package com.liwenqiang.chapter3

import com.liwenqiang.clients.producer.MockDataProducer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.{Printed, Produced}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream}
import org.apache.kafka.streams.scala.serialization.Serdes

import java.util.Properties

object KafkaStreamsYellingApp {
  def main(args: Array[String]): Unit = {

    MockDataProducer.produceRandomTextData()
    val props: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG,"yelling_app_id")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
      p
    }
    val stringSerde: Serde[String] = Serdes.stringSerde
    val builder = new StreamsBuilder()
    val simpleFirstStream: KStream[String, String] = builder.stream("src-topic")(Consumed.`with`(stringSerde, stringSerde))
    val upperCasedStream: KStream[String, String] = simpleFirstStream.mapValues((v: String) => v.toUpperCase())
    upperCasedStream.to("out-topic")(Produced.`with`(stringSerde,stringSerde))
    upperCasedStream.print(Printed.toSysOut[String,String].withLabel("Yelling App"))


    val kafkaStreams = new KafkaStreams(builder.build(), props)
    println("Hello World Yelling App Started")
    kafkaStreams.start()
    Thread.sleep(35000)
    println("Shutting down the Yelling APP now")
    kafkaStreams.close()
    MockDataProducer.shutdown()
  }
}
