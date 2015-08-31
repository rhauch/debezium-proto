/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.processor.PTopology;
import org.debezium.Testing;
import org.debezium.kafka.KafkaCluster;
import org.debezium.kafka.KafkaCluster.InteractiveConsumer;
import org.debezium.kafka.KafkaCluster.InteractiveProducer;
import org.debezium.message.Document;
import org.debezium.service.ServiceRunner.ReturnCode;
import org.fest.assertions.Fail;
import org.junit.After;
import org.junit.Before;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Base class for integration tests that verify the behavior of a service while using an embedded Kafka cluster and embedded
 * Zookeeper server. To use, simply extend this class, override the abstract methods to provide the {@link #getTopology() Kafka
 * Stream topology} and the {@link #getConsumerTopics() topics to consume} during the tests. Then, create test methods that:
 * <ol>
 * <li>{@link #startService(String...)} to start the service with the command-line arguments for the service</li>
 * <li>use the {@link #inputs()} to write messages that will be inputs for the service</li>
 * <li>use the {@link #outputs()} to read the messages from the
 * <li>perform the various operations to load messages into the input stream(s)</li>
 * <li>
 * 
 * @author Randall Hauch
 */
public abstract class ServiceIntegrationTest implements Testing {

    private KafkaCluster cluster;
    private ServiceRunner serviceRunner;
    private volatile ReturnCode actualReturnCode;
    private InteractiveProducer<String, Document> inputProducer;
    private InteractiveConsumer<String, Document> outputConsumer;
    private CountDownLatch consumerCompletion;

    @Before
    public void beforeEach() throws IOException {
        removeAllDataFiles();
        cluster = new KafkaCluster().deleteDataUponShutdown(true);
        serviceRunner = ServiceRunner.use(getClass().getName(), getTopology())
                                     .withCompletionHandler(this::setReturnCode)
                                     .withClassLoader(getClass().getClassLoader());
        cluster.startup();
        
        // Create all the topics needed during the test ...
        Set<String> topics = getAllTopics();
        cluster.createTopics(topics.toArray(new String[topics.size()]));
        
        // Create the producer and consumer...
        consumerCompletion = new CountDownLatch(1);
        inputProducer = cluster.useTo().createProducer("input-producer1");
        outputConsumer = cluster.useTo().createConsumer("output-consumer", "output-consumer", getConsumerTopics(),
                                                        consumerCompletion::countDown);
    }

    @After
    public void afterEach() {
        try {
            // First stop the producer, which happens almost immediately and while this method blocks ...
            inputProducer.close();
        } finally {
            try {
                // Then stop consuming messages; since the InteractiveConsumer caches messages in a queue, the actual
                // consumer will likely not have anything to read and will terminate pretty quickly ...
                outputConsumer.close();
            } finally {
                try {
                    // But we still have to wait for the consumer to complete before continuing ...
                    consumerCompletion.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                } finally {
                    try {
                        // Stop the service ...
                        stopService(10, TimeUnit.SECONDS);
                    } finally {
                        try {
                            // Shut down the cluster ...
                            cluster.shutdown();
                        } finally {
                            // And clean up the cluster's storage files
                            removeAllDataFiles();
                        }
                    }
                }
            }
        }
    }

    /**
     * Get the {@link InteractiveProducer producer} to write messages, presumably to the streams that are consumed by
     * the service.
     * 
     * @return the producer; never null during a test method
     */
    public InteractiveProducer<String, Document> inputs() {
        return inputProducer;
    }

    /**
     * Get the {@link InteractiveConsumer consumer} to read message from all of the {@link #getConsumerTopics() consumer topics}.
     * 
     * @return the producer; never null during a test method
     */
    public InteractiveConsumer<String, Document> outputs() {
        return outputConsumer;
    }

    /**
     * Get the {@link PTopology} class that the service-under-test uses.
     * @return the topology class; may not be null
     */
    protected abstract Class<? extends PTopology> getTopology();

    /**
     * Get the set of the all topic names used during these tests.
     * @return the topology class; may not be null
     */
    protected abstract Set<String> getAllTopics();

    /**
     * Get the set of the names for the topics {@link #outputs() read} by the test methods.
     * @return the topology class; may not be null
     */
    protected abstract Set<String> getConsumerTopics();

    private void setReturnCode(ReturnCode code) {
        actualReturnCode = code;
    }

    protected void addBrokers(int numBrokers) {
        cluster.addBrokers(numBrokers);
    }

    protected synchronized void startService(String... args) {
        // Start the ZK+Kafka cluster (if needed) ...
        if (!cluster.isRunning()) {
            try {
                cluster.startup();
            } catch (IOException e) {
                Fail.fail("Failed to start Kafka cluster", e);
            }
        }
        // Create and start a thread to run the service ...
        Thread t = new Thread(() -> serviceRunner.run(args));
        t.setName("service-thread");
        t.start();
    }

    protected synchronized void stopService(long timeout, TimeUnit unit) {
        stopService(timeout, unit, null);
    }

    protected synchronized void stopService(long timeout, TimeUnit unit, ReturnCode expectedReturnCode) {
        boolean shutdown = serviceRunner.shutdown(timeout, unit);
        if (expectedReturnCode != null) {
            assertThat(actualReturnCode).isEqualTo(expectedReturnCode);
        }
        assertThat(shutdown).isTrue();
    }

    protected boolean isServiceRunning() {
        return serviceRunner.isRunning();
    }

    private void removeAllDataFiles() {
        Testing.Files.delete("target/data/zk");
        Testing.Files.delete("target/data/kafka");
    }

}
