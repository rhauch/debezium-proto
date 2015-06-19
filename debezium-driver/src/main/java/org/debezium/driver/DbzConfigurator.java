/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.debezium.driver.Debezium.Configure;

/**
 * @author Randall Hauch
 *
 */
final class DbzConfigurator implements Debezium.Configure {
    
    private static Properties copy( Properties props ) {
        Properties copy = new Properties();
        if ( props != null && !props.isEmpty()) copy.putAll(props);
        return copy;
    }

    private final Properties props;
    private final Set<BrokerAddress> kafkaBrokerAddresses = new HashSet<>();
    private final Set<String> compressedTopics = new HashSet<>();

    DbzConfigurator() {
        this(Properties::new);
    }

    DbzConfigurator( Supplier<Properties> config ) {
        this.props = copy(config.get());
    }

    DbzConfigurator( Properties config ) {
        this.props = copy(config);
    }

    DbzConfigurator( Configuration config ) {
        this.props = config.asProperties();
    }

    private DbzConfigurator setConsumerProperty(String name, String value) {
        props.setProperty("consumers." + name, value);
        return this;
    }

    private DbzConfigurator setProducerProperty(String name, String value) {
        props.setProperty("producers." + name, value);
        return this;
    }

    @Override
    public DbzConfigurator withZookeeper(String zookeeperConnectString) {
        return setConsumerProperty("zookeeper.connect", zookeeperConnectString);
    }
    
    @Override
    public DbzConfigurator withBroker(String brokerString) {
        kafkaBrokerAddresses.add(BrokerAddress.parse(brokerString));
        return this;
    }

    @Override
    public DbzConfigurator withBroker(String brokerName, int port) {
        return withBroker(brokerName, null, port);
    }

    @Override
    public DbzConfigurator withBroker(String brokerName, String machine, int port) {
        kafkaBrokerAddresses.add(new BrokerAddress(brokerName, machine, port));
        return this;
    }

    @Override
    public DbzConfigurator acknowledgement(Debezium.Acknowledgement acknowledgement) {
        if (acknowledgement == null) acknowledgement = Debezium.Acknowledgement.NONE;
        return setProducerProperty("request.required.acks", acknowledgement.literal());
    }

    @Override
    public DbzConfigurator requestTimeout(long time, TimeUnit unit) {
        assert time > 0L;
        return setProducerProperty("request.timeout.ms", Long.toString(TimeUnit.MILLISECONDS.convert(time, unit)));
    }

    @Override
    public DbzConfigurator retryFailedRequests(int maximum) {
        assert maximum > 0;
        return setProducerProperty("message.send.max.retries", Integer.toString(maximum));
    }

    @Override
    public DbzConfigurator pauseBeforeRetries(long time, TimeUnit unit) {
        assert time > 0L;
        return setProducerProperty("retry.backoff.ms", Long.toString(TimeUnit.MILLISECONDS.convert(time, unit)));
    }

    @Override
    public DbzConfigurator refreshMetadataInterval(long time, TimeUnit unit) {
        assert time > 0L;
        return setProducerProperty("topic.metadata.refresh.interval.ms", Long.toString(TimeUnit.MILLISECONDS.convert(time, unit)));
    }

    @Override
    public DbzConfigurator compression(Debezium.Compression compression) {
        if (compression == null) compression = Debezium.Compression.NONE;
        return setProducerProperty("compression.codec", compression.literal());
    }

    @Override
    public DbzConfigurator clientId(String id) {
        if (id == null) id = "";
        return setProducerProperty("client.id", id);
    }

    @Override
    public DbzConfigurator socketBufferSize(int size) {
        assert size > 0;
        return setProducerProperty("send.buffer.bytes", Integer.toString(size));
    }
    
    
    @Override
    public Configure cleanerPeriodInSeconds(int period) {
        props.setProperty("cleaner.period.seconds", Integer.toString(period));
        return this;
    }
    
    @Override
    public Configure cleanerDelayInSeconds(int delay) {
        props.setProperty("cleaner.delay.seconds", Integer.toString(delay));
        return this;
    }
    
    @Override
    public Configure responsePartitionCount(int count) {
        props.setProperty("response.partitions", Integer.toString(count));
        return this;
    }

    @Override
    public Configure responseMaxBacklog(int count) {
        props.setProperty("response.max.backlog", Integer.toString(count));
        return this;
    }

    @Override
    public Configure initializeProducerImmediately(boolean immediately) {
        props.setProperty("initialize.producers", Boolean.toString(immediately));
        return this;
    }

    @Override
    public Configuration build() {
        if ( !kafkaBrokerAddresses.isEmpty() ) {
            setProducerProperty("metadata.broker.list",
                                kafkaBrokerAddresses.stream().map(Object::toString).collect(Collectors.joining(",")));
        }
        if (!compressedTopics.isEmpty()) {
            setProducerProperty("compressed.topics",
                                compressedTopics.stream().map(Object::toString).collect(Collectors.joining(",")));
        }
        return Configuration.from(props);
    }
}
