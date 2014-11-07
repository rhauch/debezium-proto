/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.debezium.Testing;
import org.debezium.core.doc.Document;
import org.debezium.core.util.NamedThreadFactory;
import org.fest.assertions.Fail;
import org.junit.After;
import org.junit.Before;

/**
 * @author Randall Hauch
 *
 */
public class AbstractDbzNodeTest implements Testing {

    protected DbzNode node;
    protected ExecutorService executor;
    protected ScheduledExecutorService scheduledExecutor;
    protected String metadataBrokerList = "localhost:9092";
    protected String zookeeperConnectString = "localhost:2181/";

    @Before
    public void beforeEach() {
        node = null;
        boolean useDaemonThreads = true;
        ThreadFactory threadFactory = new NamedThreadFactory("debezium", "consumer",useDaemonThreads,0,this::createdThread);
        executor = Executors.newCachedThreadPool(threadFactory);
        scheduledExecutor = Executors.newScheduledThreadPool(1);
    }
    
    @After
    public void afterEach() {
        try {
            if ( node != null ) {
                Testing.print("Beginning shutdown of node");
                node.shutdown();
                Testing.debug("Completed shutdown of node");
            }
        } finally {
            try {
                Testing.debug("Beginning shutdown of scheduledExecutor");
                scheduledExecutor.shutdown();
                Testing.debug("Completed shutdown of scheduledExecutor");
            } finally {
                try {
                    Testing.debug("Beginning shutdown of executor");
                    executor.shutdown();
                    Testing.debug("Completed shutdown of executor");
                } finally {
                    try {
                        Testing.debug("Awaiting termination of executor");
                        executor.awaitTermination(10, TimeUnit.SECONDS);
                        Testing.debug("Completed termination of executor");
                    } catch ( InterruptedException e ) {
                        // We were interrupted while blocking, so clear the status ...
                        Thread.interrupted();
                    }
                }
            }
        }
    }
    
    protected void createdThread( String threadName ) {
        // Testing.debug(Strings.getStackTrace(new RuntimeException("Created thread '" + threadName + "' (this is a trace and not an error)")));
    }
    
    protected void startWith( Document config ) {
        if ( config == null ) {
            config = Document.create();
            config.setBoolean(DbzConfiguration.INIT_PRODUCER_LAZILY,true);
        }
        node = new DbzNode(config, () -> executor,()->scheduledExecutor);
        node.start();
    }
    
    protected void sendAndReceiveMessages( int numMessages, int numThreads, String topicName, long timeout, TimeUnit unit ) throws InterruptedException {
        
        // Register a consumer on a topic ...
        CountDownLatch remaining = new CountDownLatch(numMessages);
        node.subscribe("unique", Topics.anyOf(topicName), numThreads,(topic,partition,offset,key,msg)->{
            Document doc = msg;
            long duration = System.currentTimeMillis() - doc.getLong("started");
            Testing.print("Received message '" + key + "' in " + duration + "ms");
            remaining.countDown();
            return true;
        });
        
        // Now fire a couple of messages on this topic
        IntStream.range(0, numMessages).forEach(i->{
            String key = "key" + i;
            Testing.print("Sending message '" + key + "'");
            node.send(topicName, key, Document.create("started",System.currentTimeMillis()));
        });

        // Wait till all the messages are consumed ...
        if (!remaining.await(timeout,unit)) {
            Fail.fail("Timed out while waiting for messages; received " + (numMessages-remaining.getCount()) + " of " + numMessages);
        }
        Testing.print(remaining.getCount());
    }
}
