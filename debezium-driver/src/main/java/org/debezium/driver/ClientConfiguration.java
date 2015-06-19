/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * A specialized configuration for the Debezium.Client.
 * @author Randall Hauch
 */
public interface ClientConfiguration extends Configuration {

    /**
     * Obtain a {@link ClientConfiguration} adapter for the given {@link Configuration}.
     * 
     * @param config the configuration; may not be null
     * @return the ClientConfiguration; never null
     */
    public static ClientConfiguration adapt(Configuration config) {
        if (config instanceof ClientConfiguration) return (ClientConfiguration) config;
        return new ClientConfiguration() {
            @Override
            public Set<String> keys() {
                return config.keys();
            }
            @Override
            public String getString(String key) {
                return config.getString(key);
            }

            @Override
            public String getString(String key, String defaultValue) {
                return config.getString(key, defaultValue);
            }

            @Override
            public String getString(String key, Supplier<String> defaultValueSupplier) {
                return config.getString(key, defaultValueSupplier);
            }

            @Override
            public <T> T getInstance(String key, Class<T> clazz) {
                return config.getInstance(key, clazz);
            }

            @Override
            public <T> T getInstance(String key, Class<T> clazz, Supplier<ClassLoader> classloaderSupplier) {
                return config.getInstance(key, clazz, classloaderSupplier);
            }

            @Override
            public Integer getInteger(String key) {
                return config.getInteger(key);
            }

            @Override
            public int getInteger(String key, int defaultValue) {
                return config.getInteger(key, defaultValue);
            }

            @Override
            public Integer getInteger(String key, IntSupplier defaultValueSupplier) {
                return config.getInteger(key, defaultValueSupplier);
            }

            @Override
            public Long getLong(String key) {
                return config.getLong(key);
            }

            @Override
            public long getLong(String key, long defaultValue) {
                return config.getLong(key, defaultValue);
            }

            @Override
            public Long getLong(String key, LongSupplier defaultValueSupplier) {
                return config.getLong(key, defaultValueSupplier);
            }
            @Override
            public Boolean getBoolean(String key) {
                return config.getBoolean(key);
            }
            @Override
            public boolean getBoolean(String key, boolean defaultValue) {
                return config.getBoolean(key, defaultValue);
            }
            @Override
            public Boolean getBoolean(String key, BooleanSupplier defaultValueSupplier) {
                return config.getBoolean(key, defaultValueSupplier);
            }
        };
    }

    default public long getCleanerPeriod( TimeUnit unit ) {
        return (int)unit.convert(getLong("cleaner.period.seconds",60), TimeUnit.SECONDS);
    }

    default public long getCleanerDelay( TimeUnit unit ) {
        return (int)unit.convert(getLong("cleaner.delay.seconds",60), TimeUnit.SECONDS);
    }
    
    default public boolean initializeProducersImmediately() {
        return getBoolean("initialize.producers",true);
    }
    
    default public int getResponsePartitionCount(){
        return getInteger("response.partitions",10);
    }
    
    default public int getMaxResponseBacklog(){
        return getInteger("response.max.backlog",10);
    }
    
    default public long getMaxResponseRegistrationAge( TimeUnit unit ) {
        return unit.convert(getLong("response.max.registration.age.seconds",300), TimeUnit.SECONDS);
    }
    
    default public Configuration getProducerConfiguration() {
        return subset("producer",true);
    }
    
    default public Configuration getConsumerConfiguration() {
        return subset("consumers",true);
    }
}
