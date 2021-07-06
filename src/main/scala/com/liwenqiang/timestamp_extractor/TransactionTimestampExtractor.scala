package com.liwenqiang.timestamp_extractor

import com.liwenqiang.util.model.{Purchase, StockTransaction}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor

object TransactionTimestampExtractor extends TimestampExtractor{
  override def extract(record: ConsumerRecord[Object, Object], partitionTime: Long): Long = {
    val purchaseTransaction: Purchase = record.value().asInstanceOf[Purchase]
    purchaseTransaction.getPurchaseDate.getTime
  }
}
