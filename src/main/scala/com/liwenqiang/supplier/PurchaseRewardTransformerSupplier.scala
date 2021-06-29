package com.liwenqiang.supplier

import com.liwenqiang.transformer.PurchaseRewardTransformer
import com.liwenqiang.util.model.{Purchase, RewardAccumulator}
import org.apache.kafka.streams.kstream.{ValueTransformer, ValueTransformerSupplier}

class PurchaseRewardTransformerSupplier extends ValueTransformerSupplier[Purchase ,RewardAccumulator]{
  override def get(): ValueTransformer[Purchase, RewardAccumulator] = {
    new PurchaseRewardTransformer("foobar")
  }
}
