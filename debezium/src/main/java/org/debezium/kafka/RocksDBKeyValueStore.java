/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.kafka;

import java.util.List;

import org.apache.kafka.clients.processor.ProcessorContext;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.stream.state.Entry;
import org.apache.kafka.stream.state.KeyValueIterator;
import org.apache.kafka.stream.state.KeyValueStore;

/**
 * A parameterized version of a {@link KeyValueStore} that uses RocksDB. This class wraps a
 * {@link org.apache.kafka.stream.state.RocksDBKeyValueStore} instance, which supports only {@code byte[]} key and values, and
 * uses the supplied {@link Serializer}s and {@link Deserializer}s to (de)serialize the keys and values to and from {@code byte[]}
 * s.
 * 
 * @author Randall Hauch
 * @param <K> the type of key
 * @param <V> the type of value
 * @see org.apache.kafka.stream.state.RocksDBKeyValueStore for a non-parameterized version
 */
public class RocksDBKeyValueStore<K, V> implements KeyValueStore<K, V> {

    private final org.apache.kafka.stream.state.RocksDBKeyValueStore delegate;
    private final String topic;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    public RocksDBKeyValueStore(String name, ProcessorContext context,
            Serializer<K> keySerializer, Deserializer<K> keyDeserializer,
            Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) {
        this(name,context,keySerializer,keyDeserializer, valueSerializer, valueDeserializer, new SystemTime());
    }

    public RocksDBKeyValueStore(String name, ProcessorContext context,
            Serializer<K> keySerializer, Deserializer<K> keyDeserializer,
            Serializer<V> valueSerializer, Deserializer<V> valueDeserializer, Time time) {
        delegate = new org.apache.kafka.stream.state.RocksDBKeyValueStore(name, context, time);
        this.topic = name;
        this.keySerializer = keySerializer;
        this.keyDeserializer = keyDeserializer;
        this.valueSerializer = valueSerializer;
        this.valueDeserializer = valueDeserializer;
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public void flush() {
        delegate.flush();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public boolean persistent() {
        return delegate.persistent();
    }

    protected final byte[] rawKey(K key) {
        return keySerializer.serialize(topic, key);
    }

    protected final byte[] rawValue(V value) {
        return valueSerializer.serialize(topic, value);
    }

    protected final K key(byte[] bytes) {
        return keyDeserializer.deserialize(topic, bytes);
    }

    protected final V value(byte[] bytes) {
        return valueDeserializer.deserialize(topic, bytes);
    }

    @Override
    public V get(K key) {
        return value(delegate.get(rawKey(key)));
    }

    @Override
    public void put(K key, V value) {
        delegate.put(rawKey(key), rawValue(value));
    }

    @Override
    public void putAll(List<Entry<K, V>> entries) {
        if (entries == null) return;
        for (Entry<K, V> entry : entries) {
            put(entry.key(), entry.value());
        }
    }

    @Override
    public void delete(K key) {
        delegate.delete(rawKey(key));
    }

    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        return iterate(delegate.range(rawKey(from), rawKey(to)));
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return iterate(delegate.all());
    }

    private KeyValueIterator<K, V> iterate(KeyValueIterator<byte[], byte[]> iterator) {
        return new KeyValueIterator<K, V>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Entry<K, V> next() {
                Entry<byte[], byte[]> raw = iterator.next();
                return raw == null ? null : new Entry<K, V>(key(raw.key()), value(raw.value()));
            }

            @Override
            public void close() {
                iterator.close();
            }
        };
    }

}
