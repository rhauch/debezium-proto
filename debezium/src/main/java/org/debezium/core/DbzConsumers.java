/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

/**
 * @author Randall Hauch
 */
final class DbzConsumers {
    
    @FunctionalInterface
    public static interface Consumer {
        boolean consume( String topic, int partition, long offset, String key, byte[] message );
    }

    private final DbzConfiguration config;
    private final Executor executor;
    private volatile boolean running = true;
    
    DbzConsumers( DbzConfiguration config, Executor executor ) {
        assert config != null;
        assert executor != null;
        this.config = config;
        this.executor = executor;
    }
    
    void subscribe( String groupId, TopicFilter topicFilter, int numThreads, Consumer messageConsumer ) {
        Properties props = this.config.kafkaConsumerProperties(groupId);
        boolean autoCommit = Boolean.valueOf(props.getOrDefault("auto.commit.enable","true").toString());
        ConsumerConfig config = new ConsumerConfig(props);
        ConsumerConnector connector = kafka.consumer.Consumer.createJavaConsumerConnector(config);
        Decoder<String> keyDecoder = new StringDecoder(new VerifiableProperties());
        Decoder<byte[]> valueDecoder = new DefaultDecoder(new VerifiableProperties());
        List<KafkaStream<String,byte[]>> streams = connector.createMessageStreamsByFilter(topicFilter,numThreads,keyDecoder,valueDecoder);

        // Iterate over the streams, where the number equals 'numThreads' ...
        for ( KafkaStream<String,byte[]> stream : streams ) {
            // Submit a runnable that consumes the topics ...
            executor.execute(()->{
                ConsumerIterator<String, byte[]> iter = stream.iterator();
                boolean success = false;
                while ( running ) {
                    try {
                        while ( iter.hasNext() ) {
                            MessageAndMetadata<String, byte[]> msg = iter.next();
                            success = messageConsumer.consume(msg.topic(), msg.partition(), msg.offset(), msg.key(), msg.message());
                            if ( success && autoCommit ) connector.commitOffsets();
                        }
                    } catch ( ConsumerTimeoutException e) {
                        // Keep going ...
                    }
                }
            });
        }
    }
    
    void shutdown() {
        running = false;
    }
}
