package com.liwenqiang.transformer

import com.liwenqiang.util.model.{Purchase, RewardAccumulator}
import org.apache.kafka.streams.kstream.ValueTransformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore

import java.util.Objects

//如果想采用transformValues那么必须要实现ValueTransformer接口
class PurchaseRewardTransformer(sn: String) extends ValueTransformer[Purchase, RewardAccumulator] {

  Objects.requireNonNull(sn)
  private final val storeName: String = sn
  private var stateStore: KeyValueStore[String, Int] = _
  private var context: ProcessorContext = _

  //初始化context
  override def init(context: ProcessorContext): Unit = {
    this.context = context
    this.stateStore = this.context.getStateStore(storeName)
  }

  @SuppressWarnings("deprecation")
  def punctuate(timestamp: Long): RewardAccumulator = {
    null
  }

  //转换purchase到rewardAccumulator
  override def transform(value: Purchase): RewardAccumulator = {
    val rewardAccumulator: RewardAccumulator = RewardAccumulator.builder(value).build()

    val accumulatedSoFar: Int = this.stateStore.get(rewardAccumulator.getCustomerId)

    if (accumulatedSoFar != null) {
      rewardAccumulator.addRewardPoints(accumulatedSoFar)
    }
    stateStore.put(rewardAccumulator.getCustomerId, rewardAccumulator.getTotalRewardPoints)
    rewardAccumulator
  }
  override def close(): Unit = {

  }
}
