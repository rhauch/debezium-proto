/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.client;

import static org.fest.assertions.Assertions.assertThat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.debezium.core.doc.Document;
import org.debezium.core.doc.DocumentReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Randall Hauch
 *
 */
public class DbzNodeTest {

    private DbzNode node;
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    
    @Before
    public void beforeEach() {
        node = null;
        executor = Executors.newCachedThreadPool();
        scheduledExecutor = Executors.newScheduledThreadPool(1);
    }
    
    @After
    public void afterEach() {
        try {
            if ( node != null ) {
                node.shutdown();
            }
        } finally {
            try {
                executor.shutdown();
            } finally {
                try {
                    executor.awaitTermination(10, TimeUnit.SECONDS);
                } catch ( InterruptedException e ) {
                    // We were interrupted while blocking, so clear the status ...
                    Thread.interrupted();
                }
            }
        }
    }
    
    protected void startWith( Document config ) {
        if ( config == null ) {
            config = Document.create();
            config.setBoolean(DbzConfiguration.INIT_PRODUCER_LAZILY,true);
        }
        node = new DbzNode(config, () -> executor,()->scheduledExecutor);
        node.start();
    }
    
    @Test
    public void shouldStartWhenNodeIsStarted() {
        startWith(null);
        assertThat(node.isRunning()).isTrue();
        System.out.println("Running: " + node);
    }

    @Test
    public void shouldReadPropertiesCorrectly() throws Exception {
        Document config = DocumentReader.defaultReader().read(getClass().getClassLoader().getResourceAsStream("debezium.json"));
        node = new DbzNode(config, () -> executor,()->scheduledExecutor);
    }
}
