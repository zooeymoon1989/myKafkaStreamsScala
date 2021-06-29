package com.liwenqiang.partitioner

import com.liwenqiang.util.model.Purchase
import org.apache.kafka.streams.processor.StreamPartitioner

class RewardsStreamPartitioner extends StreamPartitioner[String,Purchase]{
  override def partition(topic: String, key: String, value: Purchase, numPartitions: Int): Integer = {
    value.getCustomerId.hashCode % numPartitions
  }
}
