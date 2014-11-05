/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.client;

import static org.fest.assertions.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.debezium.Testing;
import org.debezium.client.Database.Outcome;
import org.debezium.client.DbzNode.Callable;
import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.Identifier;
import org.debezium.core.doc.Document;
import org.debezium.core.message.Message;
import org.fest.assertions.Fail;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Randall Hauch
 *
 */
public class ResponseHandlersTest implements Testing {
    
    private static final String USERNAME = "jsmith";
    
    private ResponseHandlers handlers;
    private DbzNode node;
    private ExecutorService executor;
    private ExecutionContext context;
    private DatabaseId dbId;
    private volatile RequestId requestId;
    private volatile Document response;
    private volatile CountDownLatch completed;
    
    @Before
    public void beforeEach() {
        handlers = null;
        node = null;
        executor = Executors.newCachedThreadPool();
        dbId = Identifier.of("my-db");
        context = new ExecutionContext(dbId, USERNAME);
        requestId = null;
        response = null;
        completed = null;
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
    
    protected void startWith(Document config) {
        if ( config == null ) {
            config = Document.create();
            config.setBoolean(DbzConfiguration.INIT_PRODUCER_LAZILY,true);
        }
        node = new DbzNode(config, () -> executor);
        handlers = new ResponseHandlers();
        node.add(handlers);
        node.start();
    }
    
    @Test
    public void shouldStartWhenNodeIsStarted() {
        startWith(null);
        assertThat(node.isRunning()).isTrue();
        Testing.print("Running: " + node);
    }
    
    @Test
    public void shouldDrainIncompleteRegistrationsUponShutdown() {
        startWith(null);
        assertThat(node.isRunning()).isTrue();
        Testing.print("Running: " + node);
        requestId = handlers.register(context, 1, this::noSuccess, this::noCompletion, this::expectClientStopped).orElseThrow(AssertionError::new);
        assertThat(requestId).isNotNull();
        assertThat(handlers.isEmpty()).isFalse();
        node.shutdown();
        assertThat(node.isRunning()).isFalse();
        assertThat(handlers.isEmpty()).isTrue();
    }
    
    @Test
    public void shouldGetResponseToRegistrations() {
        startWith(null);
        assertThat(node.isRunning()).isTrue();
        Testing.print("Running: " + node);
        
        // Register the handlers for a request ...
        requestId = handlers.register(context, 1, this::recordMessage, completion(), this::noFailure).orElseThrow(AssertionError::new);
        assertThat(requestId).isNotNull();
        assertThat(handlers.isEmpty()).isFalse();
        
        // Submit the response ...
        Document msg = Document.create();
        Message.addHeaders(msg, requestId.getClientId(), requestId.getRequestNumber(), context.username());
        handlers.submit(msg);
        
        // Ensure that we get the response ...
        waitForCompletion();
        assertThat(msg.equals(response)).isTrue();
    }
    
    @Test
    public void shouldGetIgnoreResponsesFromOtherClients() {
        startWith(null);
        assertThat(node.isRunning()).isTrue();
        Testing.print("Running: " + node);
        
        // Register the handlers for a request ...
        requestId = handlers.register(context, 1, this::recordMessage, completion(), this::noFailure).orElseThrow(AssertionError::new);
        assertThat(requestId).isNotNull();
        assertThat(handlers.isEmpty()).isFalse();
        
        // Submit the response from another client ...
        Document wrongMessage = Document.create();
        Message.addHeaders(wrongMessage, "other-client", requestId.getRequestNumber(), context.username());
        handlers.submit(wrongMessage); // should be discarded
        
        // Submit a real-response response ...
        Document msg = Document.create();
        Message.addHeaders(msg, requestId.getClientId(), requestId.getRequestNumber(), context.username());
        handlers.submit(msg);
        
        // Ensure that we get the response ...
        waitForCompletion();
        assertThat(msg.equals(response)).isTrue();
    }
    
    @Test
    public void shouldRequestAndWait() {
        // Testing.Print.enable();
        
        startWith(null);
        assertThat(node.isRunning()).isTrue();
        Testing.print("Running: " + node);
        
        // Register the handlers for a request ...
        boolean result = handlers.requestAndWait(context, 10, TimeUnit.SECONDS, this::submitRequest, this::processMessage, this::noFailure)
                                 .orElseThrow(AssertionError::new);
        assertThat(result).isTrue();
        assertThat(handlers.isEmpty()).isFalse();
    }
    
    void submitRequest( RequestId id ) {
        Testing.print("Submitting response ...");
        Document response = Document.create();
        Message.addHeaders(response, id.getClientId(), id.getRequestNumber(), USERNAME);
        response.setString("result", "summer");
        handlers.submit(response);
    }
    
    void noSuccess( Document doc ) {
        Fail.fail("Unexpected call to success handler with document: " + doc);
    }
    
    void recordMessage( Document doc ){
        this.response = doc;
    }
    
    boolean processMessage( Document doc ) {
        Testing.print("Processing ...");
        return doc.getString("result").equals("summer");
    }
    
    void noCompletion() {
        Fail.fail("Should not be called");
    }
    
    Callable completion() {
        this.completed = new CountDownLatch(1);
        return () -> completed.countDown();
    }
    
    void waitForCompletion() {
        try {
            if (!completed.await(10, TimeUnit.SECONDS)) {
                Fail.fail("Failed to complete in 10 seconds");
            }
        } catch (InterruptedException e) {
            Thread.interrupted();
            Fail.fail("Interrupted while waiting for completion");
        }
    }
    
    void noFailure( Outcome.Status status, String reason ) {
        Fail.fail("Unexpected failure: code=" + status + " " + reason);
    }
    
    void expectClientStopped( Outcome.Status status, String reason ) {
        if (status != Outcome.Status.CLIENT_STOPPED) Fail.fail("Unexpected failure: code=" + status + " " + reason);
    }
    
}
