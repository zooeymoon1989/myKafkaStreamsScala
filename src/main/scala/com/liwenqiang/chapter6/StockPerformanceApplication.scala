package com.liwenqiang.chapter6

import org.apache.kafka.streams.Topology

object StockPerformanceApplication {
  def main(args: Array[String]): Unit = {

    val topology = new Topology
    val stocksStateStore = "stock-performance-store"
    val differentialThreshold = 0.02

  }
}
