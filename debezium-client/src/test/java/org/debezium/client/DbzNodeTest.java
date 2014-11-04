/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.client;

import static org.fest.assertions.Assertions.assertThat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.debezium.core.doc.Document;
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
    
    @Before
    public void beforeEach() {
        node = null;
        executor = Executors.newCachedThreadPool();
    }
    
    @After
    public void afterEach() {
        if ( node != null ) {
            node.shutdown();
        }
        executor.shutdown();
    }
    
    protected void startWith( Document config ) {
        if ( config == null ) config = Document.create();
        node = new DbzNode(config,()->executor);
        node.start();
    }
    
    @Test
    public void shouldStartWhenNodeIsStarted() {
        startWith(null);
        assertThat(node.isRunning()).isTrue();
        System.out.println("Running: " + node);
    }
    
}
