/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.debezium.driver.DbzNode.Service;
import org.debezium.message.Document;
import org.debezium.message.Topic;
import org.debezium.message.Topics;
import org.debezium.model.RequestId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Randall Hauch
 *
 */
final class DbzPartialResponses extends Service {

    /**
     * Function that should be implemented to process a response document.
     * 
     * @param <R> the type of the response
     */
    @FunctionalInterface
    public static interface Responder<R> {
        /**
         * Accept the single response document. Any runtime exception will be propagated.
         * 
         * @param response the response document; never null
         * @return the result; may be null
         */
        public R accept(Document response);
    }

    /**
     * Function that should be implemented to process a partial response document. This will be called once for each
     * partial response.
     */
    @FunctionalInterface
    public static interface PartialResponder {
        /**
         * Accept a partial response document. Any runtime exception will be propagated.
         * 
         * @param response the response document; never null
         */
        public void accept(Document response);
    }

    /**
     * An object returned from {@link DbzPartialResponses#submit(Class, Consumer)}, where the caller
     * calls {@link #onResponse(long, TimeUnit, Responder)} with a {@link Responder} implementation to handle the partial
     * response.
     * 
     * @param <R> the type of the response
     */
    @FunctionalInterface
    public static interface Response<R> {
        /**
         * Perform the following operation on the response.
         * 
         * @param timeout the minimum amount of time to block for the response
         * @param unit the timeout unit; may not be null
         * @param responder the function that should be called when the response is received; may not be null
         * @return the timeout handler; never null
         */
        public TimeoutHandler<R> onResponse(long timeout, TimeUnit unit, Responder<R> responder);
    }

    /**
     * An object returned from {@link DbzPartialResponses#submit(int, Consumer)}, where the caller calls
     * {@link #onEachResponse(long, TimeUnit, PartialResponder)} with a {@link Responder} implementation to handle the partial
     * response.
     */
    @FunctionalInterface
    public static interface PartialResponse {
        /**
         * Perform the following operation on each of the response.
         * 
         * @param timeout the minimum amount of time to block for each partial response
         * @param unit the timeout unit; may not be null
         * @param responder the function that should be called when each of the responses is received; may not be null
         * @return the timeout handler; never null
         */
        public TimeoutHandler<Void> onEachResponse(long timeout, TimeUnit unit, PartialResponder responder);
    }

    /**
     * An object that the caller uses to specify the function they want to be called when the operation times out or is
     * interrupted.
     * 
     * @param <R> the type of the response
     */
    @FunctionalInterface
    public static interface TimeoutHandler<R> {
        /**
         * Perform the following operation when timing out waiting for the response. Runtime exceptions are propagated.
         * 
         * @param timeout the function to be called when the operation timed out
         * @return the response; may be null
         */
        public R onTimeout(Supplier<R> timeout);
    }

    private static <R> TimeoutHandler<R> normalCompletion(R result) {
        return runnable -> result;
    }

    private static <R> TimeoutHandler<R> timedOut() {
        return runnable -> runnable.get();
    }

    @FunctionalInterface
    static interface ResponseReceiver {
        /**
         * Receive the given partial response.
         * 
         * @param doc the response document
         * @return {@code true} if this was the last response and this receiver should be removed, or {@code false} if additional
         *         partial responses are expected.
         */
        public boolean acceptResponse(Document doc);
    }

    static final class SingleResponseReceiver<R> implements ResponseReceiver, Response<R> {
        private final RequestId id;
        private final AtomicReference<Document> response = new AtomicReference<>();
        private final CountDownLatch latch = new CountDownLatch(1);
        private final Runnable uponCompletionOrTimeout;

        SingleResponseReceiver(RequestId id, Runnable uponCompletionOrTimeout) {
            this.id = id;
            this.uponCompletionOrTimeout = uponCompletionOrTimeout;
        }

        public RequestId requestId() {
            return id;
        }

        @Override
        public TimeoutHandler<R> onResponse(long timeout, TimeUnit unit, Responder<R> responder) {
            try {
                // Wait for the response ...
                if (latch.await(timeout, unit)) {
                    // Found a response (runtime exceptions are propagated) ...
                    return normalCompletion(responder.accept(response.get()));
                }
                // Otherwise the response timed out ...
                return timedOut();
            } catch (InterruptedException e) {
                Thread.interrupted();
                return timedOut();
            } finally {
                // And always run the completion function ...
                try {
                    uponCompletionOrTimeout.run();
                } catch (Throwable t) {
                    logger.error("Error while calling completion for '{}'", requestId(), t);
                }
            }
        }

        @Override
        public boolean acceptResponse(Document doc) {
            this.response.set(doc);
            latch.countDown();
            return true;
        }
    }

    static final class MultiResponseReceiver implements ResponseReceiver, PartialResponse {
        private final RequestId id;
        private final int numResponses;
        private final AtomicLong numRemainingResponses;
        private final BlockingQueue<Document> partialResponses;
        private final Runnable uponCompletionOrTimeout;

        MultiResponseReceiver(RequestId id, int numberOfParts, Runnable uponCompletionOrTimeout) {
            this.id = id;
            this.numResponses = numberOfParts;
            this.numRemainingResponses = new AtomicLong(this.numResponses);
            this.partialResponses = new LinkedBlockingQueue<>(numberOfParts<1? 1: numberOfParts);
            this.uponCompletionOrTimeout = uponCompletionOrTimeout;
        }

        public RequestId requestId() {
            return id;
        }

        @Override
        public TimeoutHandler<Void> onEachResponse(long timeout, TimeUnit unit, PartialResponder responder) {
            try {
                for (int i = 0; i != numResponses; ++i) {
                    try {
                        Document doc = partialResponses.poll(timeout, unit);
                        if (doc != null) {
                            responder.accept(doc);
                        } else {
                            // timed out
                            return timedOut();
                        }
                    } catch (InterruptedException e) {
                        Thread.interrupted();
                        return timedOut();
                    }
                }
                return normalCompletion(null);
            } finally {
                // Always run the completion function ...
                try {
                    uponCompletionOrTimeout.run();
                } catch (Throwable t) {
                    logger.error("Error while calling completion for '{}'", requestId(), t);
                }
            }
        }

        @Override
        public boolean acceptResponse(Document doc) {
            this.partialResponses.add(doc);
            return this.numRemainingResponses.decrementAndGet() <= 0;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(DbzPartialResponses.class);
    private final ConcurrentMap<RequestId, ResponseReceiver> receivers = new ConcurrentHashMap<>();
    private volatile String clientId;
    private volatile Supplier<RequestId> requestIdSupplier;

    DbzPartialResponses() {
    }

    @Override
    public String getName() {
        return "partial-responses";
    }

    @Override
    protected void onStart(DbzNode node) {
        this.clientId = node.id();
        this.requestIdSupplier = () -> RequestId.create(clientId);

        ClientConfiguration config = ClientConfiguration.adapt(node.getConfiguration());
        int numReaderThreads = config.getResponseReaderThreadCount();

        // Subscribe to the 'partial-responses' topic with the desired number of threads ...
        logger.debug("Starting partial response service. Subscribing to '{}'...", Topic.PARTIAL_RESPONSES);
        node.subscribe(node.id(), Topics.of(Topic.PARTIAL_RESPONSES), numReaderThreads, this::processResponse);
    }

    @Override
    protected void beginShutdown(DbzNode node) {
    }

    @Override
    protected void completeShutdown(DbzNode node) {
    }

    public <R> Response<R> submit(Class<R> type, Consumer<RequestId> request) {
        RequestId requestId = requestIdSupplier.get();
        SingleResponseReceiver<R> response = new SingleResponseReceiver<>(requestId, () -> receivers.remove(requestId));
        receivers.put(requestId, response);
        try {
            request.accept(requestId);
        } catch (RuntimeException e) {
            receivers.remove(requestId);
            throw e;
        }
        return response;
    }

    public PartialResponse submit(int numberOfParts, Consumer<RequestId> request) {
        RequestId requestId = requestIdSupplier.get();
        MultiResponseReceiver response = new MultiResponseReceiver(requestId, numberOfParts, () -> receivers.remove(requestId));
        receivers.put(requestId, response);
        try {
            request.accept(requestId);
        } catch (RuntimeException e) {
            receivers.remove(requestId);
            throw e;
        }
        return response;
    }

    /**
     * Method to accept a new message read from the 'partial-responses' topic, and to forward this to the {@link ResponseReceiver}
     * registered with the same request ID. Note that each request might produce multiple partial responses, so the registered
     * handler is removed only if this response message is the last partial response received for the request.
     * 
     * @param topic the name of the topic from which the message was read
     * @param partition the partition number
     * @param offset the offset of the message
     * @param key the message key
     * @param response the partial response document; never null
     * @return {@code true} if successful, or {@code false} if the response could not be submitted because the service is
     *         no longer running or because there was not enough room in the service's queue
     */
    private boolean processResponse(String topic, int partition, long offset, String key, Document response) {
        return ifRunning(node -> {
            logger.trace("Processing partial response message from topic '{}' with key '{}': \n{}", topic, key, response);
            RequestId id = RequestId.from(response);
            ResponseReceiver receiver = receivers.get(id);
            if (receiver != null) {
                logger.trace("Calling response receiver for partial response with ID '{}' and message: \n{}", id, response);
                boolean completed = true;
                try {
                    completed = receiver.acceptResponse(response);
                } catch (Throwable t) {
                    logger.error("Error while calling response receiver for request ID '{}': {}", id, t.getMessage(), t);
                } finally {
                    if (completed) {
                        logger.trace("Removing completed response receiver for partial response with ID '{}'", id);
                        receivers.remove(id);
                    }
                }
            }
            return true;
        });
    }

}
