/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.samza;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;

/**
 * @author Randall Hauch
 * @param <K> the type of key
 * @param <V> the type of value
 */
public class MemoryKeyValueStore<K,V> implements KeyValueStore<K, V> {
    
    private final NavigableMap<K,V> map = new ConcurrentSkipListMap<>();
    private final String name;
    
    public MemoryKeyValueStore( String name ) {
        this.name = name;
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public void delete(K key) {
        map.remove(key);
    }

    @Override
    public void flush() {
        // do nothing
    }

    @Override
    public V get(K key) {
        return map.get(key);
    }

    @Override
    public void put(K key, V value) {
        map.put(key,value);
    }

    @Override
    public void putAll(List<Entry<K, V>> entries) {
        entries.forEach((entry)->put(entry.getKey(),entry.getValue()));
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return new DelegatingKeyValueIterator<K,V>(map.entrySet().iterator());
    }

    @Override
    public KeyValueIterator<K, V> range(K fromKey, K toKey) {
        return new DelegatingKeyValueIterator<K,V>(map.subMap(fromKey, toKey).entrySet().iterator());
    }
    
    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add(name + ": {");
        map.entrySet().forEach((entry)->joiner.add("" + entry.getKey() + " = " + entry.getValue()));
        joiner.add("}");
        return joiner.toString();
    }
    
    protected static class DelegatingKeyValueIterator<K,V> implements KeyValueIterator<K,V> {
        private final Iterator<Map.Entry<K,V>> delegate;
        protected DelegatingKeyValueIterator( Iterator<Map.Entry<K,V>> delegate ) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public Entry<K, V> next() {
            Map.Entry<K, V> entry = delegate.next();
            return new Entry<K,V>(entry.getKey(),entry.getValue());
        }

        @Override
        public void close() {
        }
    }
    
}
