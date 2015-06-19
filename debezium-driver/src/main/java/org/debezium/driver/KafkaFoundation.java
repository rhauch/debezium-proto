/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.io.Closeable;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.TopicFilter;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.DefaultDecoder;
import kafka.utils.VerifiableProperties;

import org.debezium.core.serde.Decoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A foundation that uses Kafka producers and consumers.
 */
final class KafkaFoundation implements Foundation {
    
    private static final class KafkaProducer implements MessageProducer, Closeable {

        private final Producer<byte[], byte[]> producer;

        public KafkaProducer(Producer<byte[], byte[]> producer) {
            this.producer = producer;
        }

        @Override
        public boolean send(KeyedMessage<byte[], byte[]> message) {
            producer.send(message);
            return true;
        }

        @Override
        public void close() {
            producer.close();
        }
    }

    private static final DefaultDecoder DEFAULT_DECODER = new DefaultDecoder(new VerifiableProperties());
    private static final MessageProducer NO_OP_PRODUCER = message->false;

    private final Properties producerConfig;
    private final Properties consumerConfig;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final AtomicReference<MessageProducer> producer = new AtomicReference<>(NO_OP_PRODUCER);
    private final Supplier<Executor> executor;
    private final Map<Properties, ConsumerConnector> connectors = new ConcurrentHashMap<>();
    private volatile boolean running = true;

    public KafkaFoundation(Configuration config, Supplier<Executor> executor) {
        ClientConfiguration clientConfig = ClientConfiguration.adapt(config);
        this.producerConfig = clientConfig.getProducerConfiguration().asProperties();
        this.consumerConfig = clientConfig.getConsumerConfiguration().asProperties();
        this.executor = executor;
        if ( clientConfig.initializeProducersImmediately() ) {
            producer();
        }
    }

    @Override
    public void shutdown() {
        // Mark this as no longer running; consumer runners will automatically terminate
        running = false;
        try {
            // Stop the producer ...
            shutdownProducer(producer.getAndUpdate(existing->NO_OP_PRODUCER));
        } finally {
            try {
                // Shutdown each of the consumer connectors ...
                connectors.values().forEach(ConsumerConnector::shutdown);
            } finally {
                connectors.clear();
            }
        }
    }

    @Override
    public MessageProducer producer() {
        return producer.updateAndGet(this::createIfMissing);
    }

    protected MessageProducer createIfMissing(MessageProducer existing) {
        if (existing == null && running) {
            try {
                logger.debug("Creating Kafka producer with config: {}", producerConfig);
                ProducerConfig pconf = new ProducerConfig(producerConfig);
                Producer<byte[], byte[]> producer = new Producer<>(pconf);
                return new KafkaProducer(producer);
            } finally {
                logger.debug("Created producer");
            }
        }
        return NO_OP_PRODUCER;
    }

    protected void shutdownProducer(MessageProducer producer) {
        if (producer instanceof KafkaProducer ) {
            try {
                logger.debug("Shutting down Kafka producer with config: {}", producerConfig);
                ((KafkaProducer)producer).close();
            } finally {
                logger.debug("Shut down producer");
            }
        }
    }

    @Override
    public <KeyType, MessageType> void subscribe(String groupId, TopicFilter topicFilter, int numThreads, Decoder<KeyType> keyDecoder,
                                                 Decoder<MessageType> messageDecoder, MessageConsumer<KeyType, MessageType> consumer) {
        if (!running) throw new IllegalStateException("Kafka client is no longer running");
        if (numThreads < 1) return;

        // Create the config for this consumer ...
        final boolean autoCommit = true;
        final boolean debug = logger.isDebugEnabled();
        Properties props = new Properties();
        props.putAll(this.consumerConfig);
        props.put("group.id", groupId);

        // Create the consumer and iterate over the streams and create a thread to process each one ...
        ConsumerConnector connector = getOrCreateConnector(props);
        connector.createMessageStreamsByFilter(topicFilter, numThreads, DEFAULT_DECODER, DEFAULT_DECODER)
                 .forEach(stream -> {
                     this.executor.get().execute(() -> {
                         final ConsumerIterator<byte[], byte[]> iter = stream.iterator();
                         boolean success = false;
                         while (true) {
                             try {
                                 while (running && iter.hasNext()) {
                                     // Determine if we're still running after we've received a message ...
                                     if ( running ) {
                                         MessageAndMetadata<byte[], byte[]> msg = iter.next();
                                         if (debug) {
                                             logger.debug("Consuming next message on topic '{}', partition {}, offset {}",
                                                          msg.topic(), msg.partition(), msg.offset());
                                         }
                                         success = consumer.consume(msg.topic(), msg.partition(), msg.offset(),
                                                                    keyDecoder.fromBytes(msg.key()),
                                                                    messageDecoder.fromBytes(msg.message()));
                                         logger.debug("Consume message: {}", success);
                                         if (success && autoCommit) {
                                             logger.debug("Committing offsets");
                                             connector.commitOffsets();
                                         }
                                     }
                                 }
                             } catch (ConsumerTimeoutException e) {
                                 logger.debug("Consumer timed out and continuing");
                                 // Keep going ...
                             }
                         }
                     });
                 });
    }

    private ConsumerConnector getOrCreateConnector(Properties props) {
        ConsumerConfig config = new ConsumerConfig(props);
        ConsumerConnector connector = connectors.get(props);
        if (connector == null) {
            logger.debug("Creating new consumer with config: {}", props);
            ConsumerConnector newConnector = kafka.consumer.Consumer.createJavaConsumerConnector(config);
            // It's possible that we and another thread might have concurrently created a consumer with the same config ...
            connector = connectors.putIfAbsent(props, newConnector);
            if (connector != null) {
                // Rare, but the new connector we created was not needed ...
                logger.debug("New consumer was not needed, so shutting down");
                executor.get().execute(() -> newConnector.shutdown());
            } else {
                logger.debug("Created new consumer with config: {}", props);
                connector = newConnector;
            }
        }
        assert connector != null;
        return connector;
    }

}
