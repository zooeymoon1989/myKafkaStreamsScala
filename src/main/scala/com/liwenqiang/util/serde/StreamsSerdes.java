package com.liwenqiang.util.serde;

import com.liwenqiang.collectors.FixedSizePriorityQueue;
import com.liwenqiang.util.model.*;
import com.liwenqiang.util.serializer.JsonDeserializer;
import com.liwenqiang.util.serializer.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class StreamsSerdes {
    public static Serde<PurchasePattern> PurchasePatternSerde() {
        return new PurchasePatternsSerde();
    }

    public static Serde<RewardAccumulator> RewardAccumulatorSerde() {
        return new RewardAccumulatorSerde();
    }

    public static Serde<Purchase> PurchaseSerde() {
        return new PurchaseSerde();
    }

    public static Serde<StockTransaction> StockTransactionSerde() {
        return new StockTransactionSerde();
    }


    public static Serde<TransactionSummary> TransactionSummarySerde() {
        return new TransactionSummarySerde();
    }


    public static final class TransactionSummarySerde extends Serdes.WrapperSerde<TransactionSummary> {
        public TransactionSummarySerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(TransactionSummary.class));
        }
    }

    public static Serde<FixedSizePriorityQueue> FixedSizePriorityQueueSerde() {
        return new FixedSizePriorityQueueSerde();
    }

    public static final class FixedSizePriorityQueueSerde extends Serdes.WrapperSerde<FixedSizePriorityQueue> {
        public FixedSizePriorityQueueSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(FixedSizePriorityQueue.class));
        }
    }

    public static final class StockTransactionSerde extends Serdes.WrapperSerde<StockTransaction> {
        public StockTransactionSerde(){
            super(new JsonSerializer<>(), new JsonDeserializer<>(StockTransaction.class));
        }
    }

    public static Serde<ShareVolume> ShareVolumeSerde() {
        return new ShareVolumeSerde();
    }

    public static final class ShareVolumeSerde extends Serdes.WrapperSerde<ShareVolume> {
        public ShareVolumeSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(ShareVolume.class));
        }
    }

    public static final class StockTickerSerde extends Serdes.WrapperSerde<StockTickerData> {
        public StockTickerSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(StockTickerData.class));
        }
    }

    public static Serde<StockTickerData> StockTickerSerde() {
        return  new StockTickerSerde();
    }
    public static final class PurchasePatternsSerde extends Serdes.WrapperSerde<PurchasePattern> {
        public PurchasePatternsSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(PurchasePattern.class));
        }
    }

    public static final class RewardAccumulatorSerde extends Serdes.WrapperSerde<RewardAccumulator> {
        public RewardAccumulatorSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(RewardAccumulator.class));
        }
    }

    public static final class PurchaseSerde extends Serdes.WrapperSerde<Purchase> {
        public PurchaseSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(Purchase.class));
        }
    }
}
