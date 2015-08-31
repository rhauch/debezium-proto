/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.kafka.clients.processor.KafkaProcessor;
import org.apache.kafka.clients.processor.ProcessorContext;
import org.apache.kafka.clients.processor.RecordCollector;
import org.apache.kafka.clients.processor.RestoreFunc;
import org.apache.kafka.clients.processor.StateStore;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.debezium.Testing;
import org.debezium.message.Document;
import org.debezium.message.DocumentSerdes;

public class MockProcessorContext<K, V> implements ProcessorContext {

    public static MockProcessorContext<String, Document> forDebezium( Consumer<ProducerRecord<String, Document>> outputReceiver ) {
        return new MockProcessorContext<String, Document>(new StringSerializer(), new StringDeserializer(),
                DocumentSerdes.INSTANCE, DocumentSerdes.INSTANCE, outputReceiver);
    }

    private final Serializer<K> keySerializer;
    private final Deserializer<K> keyDeserializer;
    private final Serializer<V> valueSerializer;
    private final Deserializer<V> valueDeserializer;
    private final Metrics metrics;
    private final RecordCollector collector;
    private String topic;
    private int partition = -1;
    private long offset = -1L;
    private long timestamp = -1L;
    private File stateDir = Testing.Files.createTestingDirectory("target/data/service/storage");

    public MockProcessorContext(Serializer<K> keySerializer, Deserializer<K> keyDeserializer, Serializer<V> valueSerializer,
            Deserializer<V> valueDeserializer, Consumer<ProducerRecord<K,V>> outputReceiver ) {
        this.keySerializer = keySerializer;
        this.keyDeserializer = keyDeserializer;
        this.valueSerializer = valueSerializer;
        this.valueDeserializer = valueDeserializer;
        this.metrics = new Metrics();
        this.collector = new RecordCollector() {
            private List<ProducerRecord<K,V>> records = new LinkedList<>();
            @SuppressWarnings("unchecked")
            @Override
            public <K1, V1> void send(ProducerRecord<K1, V1> record, Serializer<K1> keySerializer, Serializer<V1> valueSerializer) {
                records.add(new ProducerRecord<K,V>(record.topic(),(K)record.key(),(V)record.value()));
            }
            @SuppressWarnings("unchecked")
            @Override
            public void send(ProducerRecord<Object, Object> record) {
                send(new ProducerRecord<K,V>(record.topic(),(K)record.key(),(V)record.value()), keySerializer, valueSerializer);
            }
            @Override
            public void flush() {
                if ( outputReceiver != null ) records.forEach(outputReceiver::accept);
                records.clear();
            }
        };
    }
    
    public void setStateDir(File stateDir) {
        this.stateDir = stateDir;
    }
    
    public void setTopic(String topic) {
        this.topic = topic;
    }
    
    public void setPartition(int partition) {
        this.partition = partition;
    }
    
    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setTime(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean joinable(ProcessorContext other) {
        return true;
    }

    @Override
    public int id() {
        return -1;
    }

    @Override
    public Serializer<K> keySerializer() {
        return keySerializer;
    }

    @Override
    public Serializer<V> valueSerializer() {
        return valueSerializer;
    }

    @Override
    public Deserializer<K> keyDeserializer() {
        return keyDeserializer;
    }

    @Override
    public Deserializer<V> valueDeserializer() {
        return valueDeserializer;
    }

    @Override
    public RecordCollector recordCollector() {
        return this.collector;
    }

    @Override
    public File stateDir() {
        return this.stateDir;
    }

    @Override
    public Metrics metrics() {
        return metrics;
    }

    @Override
    public void register(StateStore store, RestoreFunc func) {
        // do nothing, since we don't support restoring state
    }

    @Override
    public void flush() {
        this.collector.flush();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void send(String topic, Object key, Object value) {
        this.collector.send(new ProducerRecord<K,V>(topic,(K)key,(V)value), null, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void send(String topic, Object key, Object value, Serializer<Object> keySerializer, Serializer<Object> valSerializer) {
        this.collector.send(new ProducerRecord<K,V>(topic,(K)key,(V)value),(Serializer<K>)keySerializer,(Serializer<V>)valSerializer);
    }

    @Override
    public void schedule(@SuppressWarnings("rawtypes") KafkaProcessor processor, long interval) {
        throw new UnsupportedOperationException("schedule() not supported");
    }

    @Override
    public void commit() {
        flush();
        // This context doesn't really commit offsets, so there's nothing else to do
    }

    @Override
    public String topic() {
        return this.topic;
    }

    @Override
    public int partition() {
        return this.partition;
    }

    @Override
    public long offset() {
        return this.offset;
    }

    @Override
    public long timestamp() {
        return this.timestamp;
    }

}
