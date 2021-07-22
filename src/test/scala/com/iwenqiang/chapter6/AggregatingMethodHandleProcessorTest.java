package com.iwenqiang.chapter6;

import com.iwenqiang.MockKeyValueStore;
import com.liwenqiang.processors.CogroupingProcessor;
import com.liwenqiang.util.collection.Tuple;
import com.liwenqiang.util.model.ClickEvent;
import com.liwenqiang.util.model.StockTransaction;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.Punctuator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.liwenqiang.processors.CogroupingProcessor.TUPLE_STORE_NAME;
import static org.apache.kafka.streams.processor.PunctuationType.STREAM_TIME;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

public class AggregatingMethodHandleProcessorTest {

    private org.apache.kafka.streams.processor.api.ProcessorContext<String, Tuple<ClickEvent, StockTransaction>> processorContext = mock(org.apache.kafka.streams.processor.api.ProcessorContext.class);
    private CogroupingProcessor processor = new CogroupingProcessor();
    private MockKeyValueStore<String, Tuple<List<ClickEvent>, List<StockTransaction>>> keyValueStore = new MockKeyValueStore<>();

    private ClickEvent clickEvent = new ClickEvent("ABC", "http://somelink.com", Instant.now());
    private StockTransaction transaction = StockTransaction.newBuilder().withSymbol("ABC").build();

    @Test
    @DisplayName("Proccess should initialize correctly")
    public void testInitializeCorrectly() {
        // 初始化processor
        processor.init(processorContext);
        //验证 schedule参数方法
        verify(processorContext).schedule(eq(Duration.ofMillis(15000)),eq(STREAM_TIME),isA(Punctuator.class));
        //验证获取的state store
        verify(processorContext).getStateStore(TUPLE_STORE_NAME);
    }

    @Test
    @DisplayName("Process method should store results")
    public void testProcessCorrectly(){

        when(processorContext.getStateStore(TUPLE_STORE_NAME)).thenReturn(keyValueStore);
        processor.init(processorContext);
        Record<String, Tuple<ClickEvent, StockTransaction>> record1 = new Record<>("ABC",Tuple.of(clickEvent,null),1);
        processor.process(record1);
        Tuple<List<ClickEvent>, List<StockTransaction>> tuple = keyValueStore.innerStore().get("ABC");

        assertThat(tuple._1.get(0),equalTo(clickEvent));
        assertThat(tuple._2.isEmpty(),equalTo(true));
        Record<String, Tuple<ClickEvent, StockTransaction>> record2 = new Record<>("ABC",Tuple.of(null,transaction),1);
        processor.process(record2);

        assertThat(tuple._1.get(0), equalTo(clickEvent));
        assertThat(tuple._2.get(0), equalTo(transaction));

        assertThat(tuple._1.size(), equalTo(1));
        assertThat(tuple._2.size(), equalTo(1));

    }


    @Test
    @DisplayName("Punctuate should forward records")
    public void testPunctuateProcess(){
        when(processorContext.getStateStore(TUPLE_STORE_NAME)).thenReturn(keyValueStore);
        processor.init(processorContext);
        Record<String, Tuple<ClickEvent, StockTransaction>> record1 = new Record<>("ABC",Tuple.of(clickEvent,null),1);
        Record<String, Tuple<ClickEvent, StockTransaction>> record2 = new Record<>("ABC",Tuple.of(null,transaction),1);
        processor.process(record1);
        processor.process(record2);

        Tuple<List<ClickEvent>, List<StockTransaction>> tuple = keyValueStore.innerStore().get("ABC");
        ArrayList<ClickEvent> clickEvents = new ArrayList<>(tuple._1);
        ArrayList<StockTransaction> stockTransactions = new ArrayList<>(tuple._2);
        // end is missing
        // i don't know what to do with processor
    }

}
