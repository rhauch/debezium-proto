/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.debezium.Configuration;
import org.debezium.annotation.NotThreadSafe;
import org.debezium.message.Array;
import org.debezium.message.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract KafkaProcessor for Debezium services. This base class provides automatic or manual committing of offsets, and
 * it manages the configuration properties and {@link ProcessorContext}. Debezium services may use this class if desired.
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
public abstract class ServiceProcessor implements Processor<String, Document> {

    /**
     * Utility method to create a {@link ProcessorSupplier} for a given service.
     * 
     * @param processorInstance the processor instance; may not be null
     * @return the {@link ProcessorSupplier} instance; never null
     */
    protected static ProcessorSupplier<String, Document> processorDef(ServiceProcessor processorInstance) {
        return new ProcessorSupplier<String, Document>() {

            @Override
            public Processor<String, Document> get() {
                return processorInstance;
            }
        };
    }

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Configuration config;
    private final String name;
    private boolean useManualCommit;
    private ProcessorContext context;

    /**
     * Create a new instance of the service
     * 
     * @param serviceName the name of the service; may not be null or empty
     * @param config the configuration for this processor; may not be null
     */
    protected ServiceProcessor(String serviceName, Configuration config) {
        if (serviceName == null || serviceName.trim().isEmpty()) {
            throw new IllegalArgumentException("The service name may not be null or empty");
        }
        if (config == null) throw new IllegalArgumentException("The configuration may not be null or empty");
        this.name = serviceName;
        this.config = config;
        logger.trace("Created new service '{}'", getName());
    }

    /**
     * Get the name of this service.
     * 
     * @return the name of this service; never null or empty
     */
    public String getName() {
        return name;
    }

    /**
     * Return whether this service implementation requires manually committing offsets after completely processing each
     * input message. By default this method returns {@code true}; subclasses should override this method and return {@code false}
     * if they require manual commits.
     * 
     * @return whether manual commits should be performed after each input message
     */
    protected boolean requireManaulCommit() {
        return false;
    }

    /**
     * Get the configuration for this service.
     * 
     * @return the configuration; never null
     */
    protected Configuration config() {
        return this.config;
    }

    /**
     * Get the {@link ProcessorContext} that was assigned during service initialization.
     * 
     * @return the processor context; never null
     */
    protected final ProcessorContext context() {
        return this.context;
    }

    /**
     * Initialize this processor.
     */
    @Override
    public final void init(ProcessorContext context) {
        logger.trace("Initializing service '{}'", getName());
        this.context = context;
        init();
        logger.debug("Initialized service '{}'", getName());
    }

    /**
     * Perform the initialization of the service. The {@link ProcessorContext} is available via the {@link #context()} method.
     */
    protected abstract void init();

    /**
     * Process an input message with the given key and value. This implementation will explicitly commit the offset if
     * automatic commit is not enabled.
     */
    @Override
    public final void process(String key, Document request) {
        process(context.topic(), context.partition(), context.offset(), key, request);
        if (this.useManualCommit) context.commit();
    }

    /**
     * Process an input message.
     * <p>
     * Subclasses should not explicitly {@link ProcessorContext#commit() commit offsets}, since this is done automatically by this
     * class (see {@link #requireManaulCommit()}).
     * 
     * @param topic the name of the topic from which this message was read; never null
     * @param partition the partition from which this message was read; never negative
     * @param offset the current offset of the message
     * @param key the message key; never null
     * @param request the message body; never null
     */
    protected abstract void process(String topic, int partition, long offset, String key, Document request);

    @Override
    public void punctuate(long streamTime) {
    }

    /**
     * Given the supplied {@link Array} representing a vector clock for a topic, record in the vector clock the offset in the
     * given partition and determine whether the message at the given offset has already been seen and recorded in the vector
     * clock.
     * <p>
     * This method uses the entries in the array to hold the offsets for each partition, and uses the partition as the index into
     * the array. This method automatically scales the given array if it is not large enough to hold the offset at the given
     * partition.
     * 
     * @param vectorClock the array representing a vector clock for a topic, and which is to be updated by this method
     * @param partition the partition of the input message to be recorded
     * @param offset the offset in the partition for the input message to be recorded
     * @return {@code true} if the vector clock was updated, or {@code false} if the vector clock was not updated because the
     *         given offset in the given partition has already been recorded by the vector clock
     */
    protected boolean updateVectorClock(Array vectorClock, int partition, long offset) {
        long existingOffset = vectorClock.expand(partition + 1, 0).get(partition).asLong().longValue();
        long offsetDiff = offset - existingOffset;
        if (offsetDiff <= 1) {
            vectorClock.setNumber(partition, offset);
            return true;
        }
        // Otherwise we've already seen the message at this offset ...
        return false;
    }

    protected Logger logger() {
        return logger;
    }

    protected static <T> Serializer<T> serializerFor(LongValueAccessor<T> accessor1, LongValueAccessor<T> accessor2) {
        return new Serializer<T>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public byte[] serialize(String topic, T data) {
                if (data == null) return null;

                long v1 = accessor1.value(data);
                long v2 = accessor2.value(data);
                return new byte[] {
                        (byte) (v1 >>> 56),
                        (byte) (v1 >>> 48),
                        (byte) (v1 >>> 40),
                        (byte) (v1 >>> 32),
                        (byte) (v1 >>> 24),
                        (byte) (v1 >>> 16),
                        (byte) (v1 >>> 8),
                        (byte) (v1 >>> 0),
                        (byte) (v2 >>> 56),
                        (byte) (v2 >>> 48),
                        (byte) (v2 >>> 40),
                        (byte) (v2 >>> 32),
                        (byte) (v2 >>> 24),
                        (byte) (v2 >>> 16),
                        (byte) (v2 >>> 8),
                        (byte) (v2 >>> 0)
                };
            }

            @Override
            public void close() {
            }
        };
    }

    protected static <T> Deserializer<T> deserializerFor(LongPairFactory<T> factory) {
        return new Deserializer<T>() {
            @Override
            public T deserialize(String topic, byte[] data) {
                if (data == null) return null;
                if (data.length != 16) {
                    T proto = factory.create(0, 0);
                    throw new SerializationException(
                            "Size of data received by Deserializer<" + proto.getClass().getSimpleName() + "> is not 16");
                }

                long v1 = 0;
                for (byte b : data) {
                    v1 <<= 8;
                    v1 |= b & 0xFF;
                }
                long v2 = 0;
                for (byte b : data) {
                    v2 <<= 8;
                    v2 |= b & 0xFF;
                }
                return factory.create(v1, v2);
            }

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public void close() {
            }
        };
    }

    protected static interface LongValueAccessor<T> {
        long value(T t);
    }

    protected static interface LongPairFactory<T> {
        T create(long value1, long value2);
    }
}
