package com.liwenqiang.processors;

import com.liwenqiang.Punctuator.CogroupingPunctuator;
import com.liwenqiang.util.collection.Tuple;
import com.liwenqiang.util.model.ClickEvent;
import com.liwenqiang.util.model.StockTransaction;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class CogroupingProcessor implements Processor<String, Tuple<ClickEvent, StockTransaction>, String, Tuple<ClickEvent, StockTransaction>> {
    private KeyValueStore<String, Tuple<List<ClickEvent>, List<StockTransaction>>> tupleStore;
    public static final String TUPLE_STORE_NAME = "tupleCoGroupStore";
    private ProcessorContext<String, Tuple<ClickEvent, StockTransaction>> context;
    @Override
    public void init(ProcessorContext<String, Tuple<ClickEvent, StockTransaction>> context) {
        Processor.super.init(context);
        this.context = context;
        //获取state store
        tupleStore = (KeyValueStore) context.getStateStore(TUPLE_STORE_NAME);

        CogroupingPunctuator punctuator = new CogroupingPunctuator(tupleStore, context);
        context.schedule(
                Duration.ofMillis(15000),
                PunctuationType.STREAM_TIME,
                punctuator
        );
    }

    @Override
    public void process(Record<String, Tuple<ClickEvent, StockTransaction>> record) {
        Tuple<List<ClickEvent>, List<StockTransaction>> cogroupedTuple = tupleStore.get(record.key());
        if (cogroupedTuple == null) {
            cogroupedTuple = Tuple.of(new ArrayList<>(), new ArrayList<>());
        }

        if (record.value()._1 != null) {
            cogroupedTuple._1.add(record.value()._1);
        }

        if (record.value()._2 != null) {
            cogroupedTuple._2.add(record.value()._2);
        }

        tupleStore.put(record.key(), cogroupedTuple);
    }

    @Override
    public void close() {
        Processor.super.close();
    }

    public void cogroup(long timestamp) {
        KeyValueIterator<String, Tuple<List<ClickEvent>, List<StockTransaction>>> iterator = tupleStore.all();

        while (iterator.hasNext()) {
            KeyValue<String, Tuple<List<ClickEvent>, List<StockTransaction>>> cogrouping = iterator.next();

            if (cogrouping.value != null && (!cogrouping.value._1.isEmpty() || !cogrouping.value._2.isEmpty())) {
                List<ClickEvent> clickEvents = new ArrayList<>(cogrouping.value._1);
                List<StockTransaction> stockTransactions = new ArrayList<>(cogrouping.value._2);
                Record<String, Tuple<List<ClickEvent>, List<StockTransaction>>> record = new Record<>("ABC", Tuple.of(clickEvents, stockTransactions), timestamp);
                // here is something wrong with my record
//                context.forward(record);
                cogrouping.value._1.clear();
                cogrouping.value._2.clear();
                tupleStore.put(cogrouping.key, cogrouping.value);
            }
        }
        iterator.close();
    }

}
