/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.debezium.Configuration;
import org.debezium.Testing;
import org.debezium.message.Document;
import org.debezium.message.Topics;
import org.debezium.util.NamedThreadFactory;
import org.debezium.util.Sequences;
import org.debezium.util.Strings;
import org.fest.assertions.Fail;
import org.junit.After;
import org.junit.Before;

/**
 * Abstract base class for tests that use a DbzNode instance.
 * 
 * @author Randall Hauch
 */
public abstract class AbstractDbzNodeTest implements Testing {

    protected Configuration config;
    protected DbzNode node;
    protected Environment env;
    protected Set<String> topicNames = new HashSet<>();
    private InMemoryAsyncMessageBus bus = null;

    @Before
    public void beforeEach() {
        Properties props = new Properties();
        config = Configuration.from(props);
        env = createEnvironment();
            bus = (InMemoryAsyncMessageBus) env.getMessageBus(config);
        node = new DbzNode(config, env);
        addServices(node);
        node.start();
    }

    @After
    public void afterEach() {
        try {
            if (node != null) {
                Testing.print("Beginning shutdown of node");
                node.shutdown();
                Testing.debug("Completed shutdown of node");
            }
        } finally {
            env.shutdown(10, TimeUnit.SECONDS);
        }
    }

    protected boolean useAsyncMessageBus() {
        return true;
    }
    
    protected void addServices( DbzNode node ) {
        
    }

    protected Environment createEnvironment() {
        boolean useDaemonThreads = true;
        ThreadFactory threadFactory = new NamedThreadFactory("debezium-consumer", useDaemonThreads, 0, this::createdThread);
        return Environment.build()
                          .withSecurity(MockSecurityProvider::new)
                          .withBus(this::createMessageBus)
                          .withExecutor(()->Executors.newCachedThreadPool(threadFactory))
                          .withScheduledExecutor(()->Executors.newScheduledThreadPool(1,threadFactory))
                          .create();
    }

    private MessageBus createMessageBus(Supplier<Configuration> config, Supplier<Executor> executorSupplier) {
        return useAsyncMessageBus()
                ? new InMemoryAsyncMessageBus("bus", executorSupplier)
                : new InMemorySyncMessageBus("bus");
    }

    protected void createdThread(String threadName) {
        Testing.debug(Strings.getStackTrace(new RuntimeException("Created thread '" + threadName + "' (this is a trace and not an error)")));
    }
    
    protected void createTopics( String...topicNames) {
        if (bus != null) bus.createStreams(topicNames);
    }

    protected void sendAndReceiveMessages(int numMessages, int numThreads, String topicName, long timeout, TimeUnit unit)
            throws InterruptedException {
        createTopics(topicName);

        // Register a consumer on a topic ...
        CountDownLatch remaining = new CountDownLatch(numMessages);
        node.subscribe("unique", Topics.anyOf(topicName), numThreads, (topic, partition, offset, key, msg) -> {
            Document doc = msg;
            long duration = System.currentTimeMillis() - doc.getLong("started");
            Testing.print("Received message '" + key + "' in " + duration + "ms");
            remaining.countDown();
            return true;
        });

        // Now fire a couple of messages on this topic
        Sequences.times(numMessages).forEach(i -> {
            String key = "key" + i;
            Testing.print("Sending message '" + key + "'");
            node.send(topicName, key, Document.create("started", System.currentTimeMillis()));
        });

        // Wait till all the messages are consumed ...
        if (!remaining.await(timeout, unit)) {
            Fail.fail("Timed out while waiting for messages; received " + (numMessages - remaining.getCount()) + " of " + numMessages);
        }
        Testing.print(remaining.getCount());
    }
}
