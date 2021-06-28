package com.liwenqiang.util.serde;

import com.liwenqiang.util.model.Purchase;
import com.liwenqiang.util.model.PurchasePattern;
import com.liwenqiang.util.model.RewardAccumulator;
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
