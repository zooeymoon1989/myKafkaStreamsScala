package com.liwenqiang.chapter9

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.errors.DeserializationExceptionHandler
import org.apache.kafka.streams.errors.DeserializationExceptionHandler.DeserializationHandlerResponse
import org.apache.kafka.streams.processor.ProcessorContext
import org.slf4j.{Logger, LoggerFactory}

import java.util

class DeserializerErrorHandler extends DeserializationExceptionHandler{
  val LOG:Logger = LoggerFactory.getLogger(classOf[DeserializerErrorHandler])

  override def handle(context: ProcessorContext, record: ConsumerRecord[Array[Byte], Array[Byte]], exception: Exception): DeserializationExceptionHandler.DeserializationHandlerResponse = {
    LOG.error(s"Received a deserialize error for $record cause $exception")
    DeserializationHandlerResponse.CONTINUE
  }

  override def configure(configs: util.Map[String, _]): Unit = {

  }
}
