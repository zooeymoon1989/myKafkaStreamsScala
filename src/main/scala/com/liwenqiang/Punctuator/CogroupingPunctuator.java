package com.liwenqiang.Punctuator;

import com.liwenqiang.util.collection.Tuple;
import com.liwenqiang.util.model.ClickEvent;
import com.liwenqiang.util.model.StockTransaction;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;

public class CogroupingPunctuator implements Punctuator {
    private final KeyValueStore<String,Tuple<List<ClickEvent>, List<StockTransaction>>> tupleStore;
    private final ProcessorContext<String, Tuple<ClickEvent, StockTransaction>> context;

    public CogroupingPunctuator(KeyValueStore<String,Tuple<List<ClickEvent>, List<StockTransaction>>> tupleStore, ProcessorContext<String, Tuple<ClickEvent, StockTransaction>> context){
        this.tupleStore = tupleStore;
        this.context = context;
    }

    // 定时执行的任务
    @Override
    public void punctuate(long timestamp) {
        KeyValueIterator<String, Tuple<List<ClickEvent>, List<StockTransaction>>> iterator = tupleStore.all();

        while (iterator.hasNext()) {
            KeyValue<String, Tuple<List<ClickEvent>, List<StockTransaction>>> cogrouped = iterator.next();
            if (cogrouped.value != null && (!cogrouped.value._1.isEmpty() || !cogrouped.value._2.isEmpty())) {
                List<ClickEvent> clickEvents = new ArrayList<>(cogrouped.value._1);
                List<StockTransaction> stockTransactions = new ArrayList<>(cogrouped.value._2);
                Tuple<List<ClickEvent>, List<StockTransaction>> tuple = Tuple.of(clickEvents, stockTransactions);
                Record<String, Tuple<ClickEvent, StockTransaction>> record = new Record<>(cogrouped.key, new Tuple<ClickEvent, StockTransaction>(null, null), timestamp);
                record.withValue(tuple);
                context.forward(record);
                // empty out the current cogrouped results
                cogrouped.value._1.clear();
                cogrouped.value._2.clear();
                tupleStore.put(cogrouped.key, cogrouped.value);
            }
        }

    }
}
