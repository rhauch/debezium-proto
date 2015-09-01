/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.processor.KafkaProcessor;
import org.apache.kafka.clients.processor.ProcessorContext;
import org.apache.kafka.clients.processor.ProcessorProperties;
import org.debezium.annotation.NotThreadSafe;
import org.debezium.message.Array;
import org.debezium.message.Document;

/**
 * An abstract KafkaProcessor for Debezium services. This base class provides automatic or manual committing of offsets, and
 * it manages the configuration properties and {@link ProcessorContext}. Debezium services may use this class if desired.
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
public abstract class ServiceProcessor extends KafkaProcessor<String, Document, String, Document> {

    private final boolean useManualCommit;
    private final ProcessorProperties config;
    private ProcessorContext context;

    /**
     * Create a new instance of the service
     * 
     * @param serviceName the name of the service; may not be null or empty
     * @param config the processor configuration; may not be null
     */
    protected ServiceProcessor(String serviceName, ProcessorProperties config) {
        super(serviceName);
        if (serviceName == null || serviceName.trim().isEmpty()) {
            throw new IllegalArgumentException("The service name may not be null or empty");
        }
        if (config == null) throw new IllegalArgumentException("The processor properties may not be null");
        this.config = config;

        // Look in the processor properties for any process-specific configuration ...
        boolean autoCommitEnabled = property(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        int autoCommitIntervalMs = property(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 60000);
        this.useManualCommit = requireManaulCommit() || !autoCommitEnabled || autoCommitIntervalMs < 1;
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
     * Get the named configuration property as a boolean value. This method is safe to be called within a constructor.
     * 
     * @param name the name of the configuration property; may not be null
     * @param defaultValue the default value to be returned if the configuration properties do not contain the named property
     * @return the value of the configuration property, or the supplied default if there is none
     */
    protected boolean property(String name, boolean defaultValue) {
        return Boolean.parseBoolean(property(name, Boolean.toString(defaultValue)));
    }

    /**
     * Get the named configuration property as an integer value. This method is safe to be called within a constructor.
     * 
     * @param name the name of the configuration property; may not be null
     * @param defaultValue the default value to be returned if the configuration properties do not contain the named property
     * @return the value of the configuration property, or the supplied default if there is none
     */
    protected int property(String name, int defaultValue) {
        return Integer.parseInt(property(name, Integer.toString(defaultValue)));
    }

    /**
     * Get the named configuration property as a long value. This method is safe to be called within a constructor.
     * 
     * @param name the name of the configuration property; may not be null
     * @param defaultValue the default value to be returned if the configuration properties do not contain the named property
     * @return the value of the configuration property, or the supplied default if there is none
     */
    protected long property(String name, long defaultValue) {
        return Long.parseLong(property(name, Long.toString(defaultValue)));
    }

    /**
     * Get the named configuration property as a float value. This method is safe to be called within a constructor.
     * 
     * @param name the name of the configuration property; may not be null
     * @param defaultValue the default value to be returned if the configuration properties do not contain the named property
     * @return the value of the configuration property, or the supplied default if there is none
     */
    protected float property(String name, float defaultValue) {
        return Float.parseFloat(property(name, Float.toString(defaultValue)));
    }

    /**
     * Get the named configuration property as a double value. This method is safe to be called within a constructor.
     * 
     * @param name the name of the configuration property; may not be null
     * @param defaultValue the default value to be returned if the configuration properties do not contain the named property
     * @return the value of the configuration property, or the supplied default if there is none
     */
    protected double property(String name, double defaultValue) {
        return Double.parseDouble(property(name, Double.toString(defaultValue)));
    }

    /**
     * Get the named configuration property as a String value. This method is safe to be called within a constructor or any
     * other methods. The configuration properties never change during the lifetime of this object.
     * 
     * @param name the name of the configuration property; may not be null
     * @param defaultValue the default value to be returned if the configuration properties do not contain the named property
     * @return the value of the configuration property, or the supplied default if there is none
     */
    protected String property(String name, String defaultValue) {
        return this.config.config().getProperty(name, defaultValue);
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
        this.context = context;
        init();
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

}
