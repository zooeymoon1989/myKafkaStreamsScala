package com.liwenqiang.interceptors;

import org.apache.commons.lang3.text.StrBuilder;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

public class StockTransactionConsumerInterceptor implements ConsumerInterceptor<Object, Object> {

    private static final Logger LOG = LoggerFactory.getLogger(StockTransactionConsumerInterceptor.class);

    public StockTransactionConsumerInterceptor() {
        LOG.info("Built StockTransactionConsumerInterceptor");
    }

    @Override
    public ConsumerRecords<Object, Object> onConsume(ConsumerRecords<Object, Object> records) {
        LOG.info("Intercepted ConsumerRecords {}", buildMessage(records.iterator()));
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        LOG.info("Commit information {}", offsets);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    private String buildMessage(Iterator<ConsumerRecord<Object, Object>> consumerRecords) {
        StrBuilder builder = new StrBuilder();
        while (consumerRecords.hasNext()) {
            builder.append(consumerRecords.next());
        }
        return builder.toString();
    }
}
