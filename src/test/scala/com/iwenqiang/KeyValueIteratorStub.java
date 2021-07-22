package com.iwenqiang;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Iterator;

public class KeyValueIteratorStub<K, V> implements KeyValueIterator<K, V> {

    private final Iterator<KeyValue<K, V>> iterator;

    public KeyValueIteratorStub(final Iterator<KeyValue<K, V>> iterator) {
        this.iterator = iterator;
    }

    @Override
    public void close() {
        //no-op
    }

    @Override
    public K peekNextKey() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public KeyValue<K, V> next() {
        return iterator.next();
    }

}
