package com.liwenqiang.joiner

import com.liwenqiang.util.model.{CorrelatedPurchase, Purchase}
import org.apache.kafka.streams.kstream.ValueJoiner

import java.util
import java.util.Date

class PurchaseJoiner extends ValueJoiner[Purchase,Purchase,CorrelatedPurchase]{

  override def apply(purchase: Purchase, otherPurchase: Purchase): CorrelatedPurchase = {
    val builder: CorrelatedPurchase.Builder = CorrelatedPurchase.newBuilder()
    val purchaseDate: Serializable = if (purchase != null) purchase.getPurchaseDate else Option(null)
    val price: Any = if (purchase != null) purchase.getPrice else Option(null)
    val itemPurchased: Serializable = if (purchase != null) purchase.getItemPurchased else Option(null)
    val otherPurchaseDate: Serializable = if (otherPurchase != null) otherPurchase.getPurchaseDate else Option(null)
    val otherPrice: Any = if (otherPurchase != null) otherPurchase.getPrice else Option(null)
    val otherItemPurchased: Serializable = if (otherPurchase != null) otherPurchase.getItemPurchased else Option(null)
    val purchasedItems = new util.ArrayList[String]()
    if (itemPurchased != null) purchasedItems.add(itemPurchased.asInstanceOf[String])
    if (otherItemPurchased != null) purchasedItems.add(otherItemPurchased.asInstanceOf[String])
    val customerId: Serializable = if (purchase != null) purchase.getCustomerId else Option(null)
    val otherCustomerId: Serializable = if (otherPurchase != null) otherPurchase.getCustomerId else Option(null)

    builder.withCustomerId(if(customerId != null) customerId.asInstanceOf[String] else otherCustomerId.asInstanceOf[String])

    if (purchaseDate !=null) {
      builder.withFirstPurchaseDate(purchaseDate.asInstanceOf[Date])
    }
    if (otherPurchase !=null) {
      builder.withFirstPurchaseDate(otherPurchaseDate.asInstanceOf[Date])
    }

    builder.withItemsPurchased(purchasedItems)
      .withTotalAmount(price.asInstanceOf[Double]+otherPrice.asInstanceOf[Double]).build()
  }
}
