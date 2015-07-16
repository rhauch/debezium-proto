/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.debezium.core.util.NamedThreadFactory;
import org.debezium.driver.Debezium.Acknowledgement;
import org.debezium.driver.Debezium.Builder;
import org.debezium.driver.Debezium.Compression;

/**
 * @author Randall Hauch
 *
 */
final class DbzDriverBuilder implements Builder {

    private final Properties props = new Properties();
    private final Set<BrokerAddress> kafkaBrokerAddresses = new HashSet<>();
    private final Set<String> compressedTopics = new HashSet<>();
    private Function<Supplier<Executor>, MessageBus> busFactory = null;
    private Supplier<ExecutorService> executorFactory = null;
    private Supplier<ScheduledExecutorService> scheduledExecutorFactory = null;
    private Supplier<SecurityProvider> securityFactory = null;

    DbzDriverBuilder() {
    }

    private DbzDriverBuilder setConsumerProperty(String name, String value) {
        props.setProperty("consumers." + name, value);
        return this;
    }

    private DbzDriverBuilder setProducerProperty(String name, String value) {
        props.setProperty("producers." + name, value);
        return this;
    }
    
    @Override
    public DbzDriverBuilder load(Properties properties) {
        for ( String propName : properties.stringPropertyNames() ) {
            props.setProperty(propName,properties.getProperty(propName));
        }
        return this;
    }

    @Override
    public DbzDriverBuilder withZookeeper(String zookeeperConnectString) {
        return setConsumerProperty("zookeeper.connect", zookeeperConnectString);
    }
    
    @Override
    public DbzDriverBuilder withBroker(String brokerName, String machine, int port) {
        kafkaBrokerAddresses.add(new BrokerAddress(brokerName, machine, port));
        return this;
    }

    @Override
    public DbzDriverBuilder acknowledgement(Acknowledgement acknowledgement) {
        if (acknowledgement == null) acknowledgement = Acknowledgement.NONE;
        return setProducerProperty("request.required.acks", acknowledgement.literal());
    }

    @Override
    public DbzDriverBuilder requestTimeout(long time, TimeUnit unit) {
        assert time > 0L;
        return setProducerProperty("request.timeout.ms", Long.toString(TimeUnit.MILLISECONDS.convert(time, unit)));
    }

    @Override
    public DbzDriverBuilder retryFailedRequests(int maximum) {
        assert maximum > 0;
        return setProducerProperty("message.send.max.retries", Integer.toString(maximum));
    }

    @Override
    public DbzDriverBuilder pauseBeforeRetries(long time, TimeUnit unit) {
        assert time > 0L;
        return setProducerProperty("retry.backoff.ms", Long.toString(TimeUnit.MILLISECONDS.convert(time, unit)));
    }

    @Override
    public DbzDriverBuilder refreshMetadataInterval(long time, TimeUnit unit) {
        assert time > 0L;
        return setProducerProperty("topic.metadata.refresh.interval.ms", Long.toString(TimeUnit.MILLISECONDS.convert(time, unit)));
    }

    @Override
    public DbzDriverBuilder compression(Compression compression) {
        if (compression == null) compression = Compression.NONE;
        return setProducerProperty("compression.codec", compression.literal());
    }

    @Override
    public DbzDriverBuilder clientId(String id) {
        if (id == null) id = "";
        return setProducerProperty("client.id", id);
    }

    @Override
    public DbzDriverBuilder socketBufferSize(int size) {
        assert size > 0;
        return setProducerProperty("send.buffer.bytes", Integer.toString(size));
    }
    
    @Override
    public DbzDriverBuilder responsePartitionCount(int count) {
        props.setProperty("response.partition.count", Integer.toString(count));
        return this;
    }
    
    @Override
    public DbzDriverBuilder responseMaxBacklog(int count) {
        props.setProperty("response.max.backlog", Integer.toString(count));
        return this;
    }

    @Override
    public DbzDriverBuilder initializeProducerImmediately(boolean immediately) {
        props.setProperty("initialize.producers", Boolean.toString(immediately));
        return this;
    }

    @Override
    public DbzDriverBuilder usingSecurity(Supplier<SecurityProvider> security) {
        securityFactory = security;
        return this;
    }

    @Override
    public DbzDriverBuilder usingBus(Function<Supplier<Executor>, MessageBus> messageBus) {
        busFactory = messageBus;
        return this;
    }

    @Override
    public DbzDriverBuilder usingExecutor(Supplier<ExecutorService> executor) {
        executorFactory = executor;
        return this;
    }

    @Override
    public DbzDriverBuilder usingScheduledExecutor(Supplier<ScheduledExecutorService> executor) {
        scheduledExecutorFactory = executor;
        return this;
    }

    @Override
    public Debezium start() {
        if ( !kafkaBrokerAddresses.isEmpty() ) {
            setProducerProperty("metadata.broker.list",
                                kafkaBrokerAddresses.stream().map(Object::toString).collect(Collectors.joining(",")));
        }
        if (!compressedTopics.isEmpty()) {
            setProducerProperty("compressed.topics",
                                compressedTopics.stream().map(Object::toString).collect(Collectors.joining(",")));
        }
        Configuration config = Configuration.from(props);
        if ( executorFactory == null ) {
            boolean useDaemonThreads = false; // they will keep the VM running if not shutdown properly
            ThreadFactory threadFactory = new NamedThreadFactory("debezium", "consumer", useDaemonThreads);
            executorFactory = ()-> Executors.newCachedThreadPool(threadFactory);
        }
        if ( scheduledExecutorFactory == null ) {
            ThreadFactory scheduledThreadFactory = new NamedThreadFactory("debezium", "timer", true);
            scheduledExecutorFactory = ()-> Executors.newScheduledThreadPool(0, scheduledThreadFactory);
        }
        if ( busFactory == null ) {
            busFactory = (execSupplier) -> new KafkaMessageBus(config, execSupplier);
        }
        if ( securityFactory == null ) {
            // TODO: Change this to use a real security provider implementation by default
            securityFactory = () -> new PassthroughSecurityProvider();
        }
        Environment env = new Environment(securityFactory, executorFactory, scheduledExecutorFactory, busFactory);
        return new DbzDriver(config,env).start();
    }

}
