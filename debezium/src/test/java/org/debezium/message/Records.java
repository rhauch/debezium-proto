/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.message;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Utility methods for writing a series of {@link ProducerRecord}s to a JSON file and reading them back in as
 * {@link ConsumerRecords}.
 * 
 * @author Randall Hauch
 */
public class Records {

    /**
     * Write a set of records to the given JSON {@link Array} file.
     * 
     * @param file the file to which the records are to be written; may not be null
     * @param records the stream of records to be written; may not be null
     * @throws IOException if an error occurs while reading the file
     */
    public static void write(File file, Stream<ProducerRecord<String, Document>> records) throws IOException {
        try (OutputStream stream = new FileOutputStream(file)) {
            ArrayWriter writer = ArrayWriter.prettyWriter();
            Offsets offsets = new Offsets();
            writer.write(wrap(records, offsets::nextOffset), stream);
        }
    }

    protected static Array wrap(Stream<ProducerRecord<String, Document>> records, Function<String, Long> offsets) {
        Array result = Array.create();
        records.forEach(record -> result.add(write(record, offsets)));
        return result;
    }

    protected static Document write(ProducerRecord<String, Document> record, Function<String, Long> offsets) {
        Document wrapper = Document.create();
        Integer partition = record.partition();
        wrapper.setString("topic", record.topic());
        if ( partition != null ) wrapper.setNumber("partition", partition);
        wrapper.setNumber("offset", offsets.apply(record.topic()));
        wrapper.setString("key", record.key());
        wrapper.setDocument("value", record.value());
        return wrapper;
    }

    /**
     * Read a set of records from the given JSON {@link Array} file and return the {@link ConsumerRecords}.
     * 
     * @param file the file from which the records are to be read; may not be null
     * @return the records that were read in from the file; never null
     * @throws IOException if an error occurs while reading the file
     */
    public static ConsumerRecords<String, Document> read(File file) throws IOException {
        try (InputStream stream = new FileInputStream(file)) {
            return read(stream);
        }
    }

    /**
     * Read a set of records from the given JSON {@link Array} stream and return the {@link ConsumerRecords}.
     * 
     * @param stream the stream from which the records are to be read; may not be null
     * @return the records that were read in from the file; never null
     * @throws IOException if an error occurs while reading the file
     */
    public static ConsumerRecords<String, Document> read(InputStream stream) throws IOException {
        try {
            ConcurrentMap<TopicPartition, List<ConsumerRecord<String, Document>>> records = new ConcurrentHashMap<>();
            TopicPartitions tps = new TopicPartitions();
            ArrayReader.defaultReader().readArray(stream).forEach(entry -> {
                Document wrapper = entry.getValue().asDocument();
                if (wrapper != null) {
                    String topic = wrapper.getString("topic");
                    int partition = wrapper.getInteger("partition",0);
                    long offset = wrapper.getLong("offset", 0L);
                    String key = wrapper.getString("key");
                    Document value = wrapper.getDocument("value");
                    ConsumerRecord<String, Document> record = new ConsumerRecord<>(topic, partition, offset, key, value);
                    TopicPartition tp = tps.given(topic, partition);
                    records.computeIfAbsent(tp, k -> new ArrayList<ConsumerRecord<String, Document>>()).add(record);
                }
            });
            return new ConsumerRecords<>(records);
        } finally {
            stream.close();
        }
    }

    private static class TopicPartitions {
        private TopicPartition last;

        public TopicPartition given(String topic, int partition) {
            if (last != null && last.topic().equals(topic) && last.partition() == partition) return last;
            last = new TopicPartition(topic, partition);
            return last;
        }
    }

    private static class Offsets {
        private final ConcurrentMap<String, AtomicLong> counters = new ConcurrentHashMap<>();

        public long nextOffset(String topic) {
            return counters.computeIfAbsent(topic, t -> new AtomicLong()).incrementAndGet();
        }
    }

    private Records() {
    }

}
