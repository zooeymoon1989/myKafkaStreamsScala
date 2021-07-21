package com.liwenqiang.restore;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;

import java.text.DecimalFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LoggingStateRestoreListener implements StateRestoreListener {

    private static final Map<TopicPartition, Long> totalToRestore = new ConcurrentHashMap<>();
    private static final Map<TopicPartition, Long> restoredSoFar = new ConcurrentHashMap<>();

    @Override
    public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
        long toRestore = endingOffset - startingOffset;
        totalToRestore.put(topicPartition, toRestore);
        System.out.printf("Starting restoration for %s on topic-partition %s total to restore %d\n", storeName, topicPartition, toRestore);
    }

    @Override
    public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
        DecimalFormat formatter = new DecimalFormat("#.##");
        long currentProgress = numRestored + restoredSoFar.getOrDefault(topicPartition, 0L);
        double percentComplete = (double) currentProgress / totalToRestore.get(topicPartition);
        System.out.printf("Completed %d for %s%% of total restoration for %s on %s\n", numRestored, formatter.format(percentComplete * 100.00), storeName, topicPartition);
    }

    @Override
    public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
        System.out.printf("Restoration completed for %s on topic-partition %s\n", storeName, topicPartition);
        restoredSoFar.put(topicPartition, 0L);
    }
}
