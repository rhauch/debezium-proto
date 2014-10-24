/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.debezium.api.Debezium;
import org.debezium.api.Debezium.Configure;

/**
 * @author Randall Hauch
 *
 */
public final class DbzConfigurator implements Configure {

    private final Properties producerProps = new Properties();
    private final Properties consumerProps = new Properties();
    private final Set<BrokerAddress> kafkaBrokerAddresses = new HashSet<>();
    private final Set<String> compressedTopics = new HashSet<>();
    public DbzConfigurator() {
    }

    @Override
    public Configure withZookeeper(String zookeeperConnectString) {
        consumerProps.put("zookeeper.connect", zookeeperConnectString);
        return this;
    }

    @Override
    public Configure withBroker(String brokerString) {
        kafkaBrokerAddresses.add(BrokerAddress.parse(brokerString));
        return this;
    }
    @Override
    public Configure withBroker(String brokerName, int port) {
        return withBroker(brokerName,null,port);
    }
    @Override
    public Configure withBroker(String brokerName, String machine, int port) {
        kafkaBrokerAddresses.add(new BrokerAddress(brokerName,machine,port));
        return this;
    }
    @Override
    public Configure acknowledgement(Debezium.Acknowledgement acknowledgement) {
        if ( acknowledgement == null ) acknowledgement = Debezium.Acknowledgement.NONE;
        producerProps.setProperty("request.required.acks", acknowledgement.literal());
        return this;
    }
    @Override
    public Configure requestTimeout(long time, TimeUnit unit) {
        assert time > 0L;
        producerProps.setProperty("request.timeout.ms", Long.toString(TimeUnit.MILLISECONDS.convert(time, unit)));
        return this;
    }
    @Override
    public Configure retryFailedRequests(int maximum) {
        assert maximum > 0;
        producerProps.setProperty("message.send.max.retries", Integer.toString(maximum));
        return this;
    }
    @Override
    public Configure pauseBeforeRetries(long time, TimeUnit unit) {
        assert time > 0L;
        producerProps.setProperty("retry.backoff.ms", Long.toString(TimeUnit.MILLISECONDS.convert(time, unit)));
        return this;
    }
    @Override
    public Configure refreshMetadataInterval(long time, TimeUnit unit) {
        assert time > 0L;
        producerProps.setProperty("topic.metadata.refresh.interval.ms", Long.toString(TimeUnit.MILLISECONDS.convert(time, unit)));
        return this;
    }
    @Override
    public Configure compression(Debezium.Compression compression) {
        if ( compression == null ) compression = Debezium.Compression.NONE;
        producerProps.setProperty("compression.codec", compression.literal());
        return this;
    }
    @Override
    public Configure clientId(String id) {
        if ( id == null ) id = "";
        producerProps.setProperty("client.id", id);
        return this;
    }
    @Override
    public Configure socketBufferSize(int size) {
        assert size > 0;
        producerProps.setProperty("send.buffer.bytes", Integer.toString(size));
        return this;
    }
    @Override
    public Debezium.Configuration build() {
//        producerProps.put("serializer.class","kafka.serializer.StringEncoder");
//        producerProps.put("serializer.class",DebeziumSerializer.class.getName());
        producerProps.setProperty("metadata.broker.list",
                                kafkaBrokerAddresses.stream().map(Object::toString).collect(Collectors.joining(",")));
        if ( !compressedTopics.isEmpty() ) {
            producerProps.setProperty("compressed.topics",
                    compressedTopics.stream().map(Object::toString).collect(Collectors.joining(",")));
        }
        return new DbzConfiguration(producerProps,consumerProps);
    }
}
