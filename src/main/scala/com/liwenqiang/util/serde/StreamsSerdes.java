package com.liwenqiang.util.serde;

import com.google.gson.reflect.TypeToken;
import com.liwenqiang.collectors.FixedSizePriorityQueue;
import com.liwenqiang.util.collection.Tuple;
import com.liwenqiang.util.model.*;
import com.liwenqiang.util.serializer.JsonDeserializer;
import com.liwenqiang.util.serializer.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.lang.reflect.Type;
import java.util.List;

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
    public static Serde<Tuple<List<ClickEvent>, List<StockTransaction>>> EventTransactionTupleSerde() {
        return new EventTransactionTupleSerde();
    }

    public static final class EventTransactionTupleSerde extends Serdes.WrapperSerde<Tuple<List<ClickEvent>, List<StockTransaction>>> {
        private static final Type tupleType = new TypeToken<Tuple<List<ClickEvent>, List<StockTransaction>>>(){}.getType();
        public EventTransactionTupleSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(tupleType));
        }
    }

    public static Serde<ClickEvent> ClickEventSerde() {
        return new ClickEventSerde();
    }


    public static final class ClickEventSerde extends Serdes.WrapperSerde<ClickEvent> {
        public ClickEventSerde () {
            super(new JsonSerializer<>(), new JsonDeserializer<>(ClickEvent.class));
        }
    }

    public static Serde<TransactionSummary> TransactionSummarySerde() {
        return new TransactionSummarySerde();
    }

    public static Serde<StockPerformance> StockPerformanceSerde() {
        return new StockPerformanceSerde();
    }
    public static final class StockPerformanceSerde extends Serdes.WrapperSerde<StockPerformance> {
        public StockPerformanceSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(StockPerformance.class));
        }
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
