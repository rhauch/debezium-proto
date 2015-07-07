/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongToIntFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.debezium.core.doc.Document;
import org.debezium.core.function.Callable;
import org.debezium.core.util.Sequences;
import org.debezium.driver.Database.Outcome;
import org.debezium.driver.DbzNode.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service that manages a set of handlers for response messages. The service partitions the handlers and response messages
 * by {@link RequestId}, and uses a single thread per partition to ensure that all responses for a given request are processed
 * by a single thread.
 * 
 * @author Randall Hauch
 */
final class ResponseHandlers extends Service {

    public static int DEFAULT_MAX_REGISTRATION_AGE_IN_SECONDS = 300;

    public static Handlers with(Consumer<Document> successHandler, Callable completionHandler,
                                BiConsumer<Outcome.Status, String> failureHandler) {
        return new Handlers(successHandler, completionHandler, failureHandler);
    }

    public static final class Handlers {
        public final Optional<Consumer<Document>> successHandler;
        public final Optional<Callable> completionHandler;
        public final Optional<BiConsumer<Outcome.Status, String>> failureHandler;

        protected Handlers(Consumer<Document> successHandler, Callable completionHandler, BiConsumer<Outcome.Status, String> failureHandler) {
            this.successHandler = Optional.ofNullable(successHandler);
            this.completionHandler = Optional.ofNullable(completionHandler);
            this.failureHandler = Optional.ofNullable(failureHandler);
        }

        @Override
        public String toString() {
            return successHandler + " & " + completionHandler + " & " + failureHandler;
        }
    }

    /**
     * A handler of response message(s) for a given request.
     */
    public static interface ResponseHandler {
        /**
         * Handle the response message. This message may be one of several parts to the original request, and this method
         * will typically be called once for each partial response (unless an
         * {@link #handleError(org.debezium.driver.Database.Outcome.Status, String)} occurs).
         * 
         * @param response the response message; never null
         */
        public void handleResponse(Document response);

        /**
         * Handle a failure condition. Once this method is called, the handler will be unregistered.
         * 
         * @param status the failure status
         * @param failureReason the reasons for the failure
         */
        public void handleError(Outcome.Status status, String failureReason);
    }

    private static final class Registration {
        protected final ExecutionContext context;
        protected final Handlers handlers;
        private final AtomicInteger partsRemaining;
        private final long registeredAt;

        protected Registration(ExecutionContext context, int parts, Handlers handlers) {
            this.context = context;
            this.handlers = handlers;
            this.partsRemaining = new AtomicInteger(parts);
            this.registeredAt = System.currentTimeMillis();
        }

        /**
         * Call the handler for the given response, and
         * 
         * @param requestId the request ID; never null
         * @param response the response message; never null
         * @param onCompletion the function that should be called if this is the last response; never null
         */
        protected void handle(RequestId requestId, Document response, Callable onCompletion) {
            handlers.successHandler.ifPresent(f -> f.accept(response));
            if (partsRemaining.decrementAndGet() <= 0) {
                handlers.completionHandler.ifPresent(Callable::call);
                onCompletion.call();
            }
        }

        protected void fail(Outcome.Status status, String failureMessage) {
            handlers.failureHandler.ifPresent(f -> f.accept(status, failureMessage));
        }

        public long age(long now) {
            return now - this.registeredAt;
        }

        @Override
        public String toString() {
            return context + " (" + handlers + ")";
        }
    }

    /**
     * The queue of response messages for a subset of requests.
     * @author Randall Hauch
     */
    private static final class Partition {
        private final Logger logger = LoggerFactory.getLogger(getClass());
        private final BlockingQueue<Document> queue;
        private final Runnable runnable;
        private final AtomicBoolean run = new AtomicBoolean(true);

        protected Partition(int maxBacklog, Consumer<Document> consumer, Consumer<Partition> onStartup, Consumer<Partition> onCompletion) {
            this.queue = new LinkedBlockingDeque<Document>(maxBacklog);
            this.runnable = new Runnable() {
                @Override
                public void run() {
                    onStartup.accept(Partition.this);
                    while (run.get()) {
                        try {
                            Document doc = queue.poll(500, TimeUnit.MILLISECONDS);
                            if (doc != null) {
                                consumer.accept(doc);
                            }
                        } catch (InterruptedException e) {
                            Thread.interrupted(); // clear the flag for this thread ...
                            break;
                        }
                    }
                    onCompletion.accept(Partition.this);
                }
            };
        }

        public boolean submit(Document response) {
            logger.trace("Submitting response message to the partition queue");
            return queue.offer(response);
        }

        public boolean submit(Document response, long timeout, TimeUnit unit) throws InterruptedException {
            logger.trace("Submitting response message to the partition queue with max timeout of {} {}",timeout,unit);
            return queue.offer(response, timeout, unit);
        }

        public void stop() {
            logger.debug("Stopping partition");
            this.run.set(false);
        }
    }

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final List<Partition> partitions = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<RequestId, Registration> registrations = new ConcurrentHashMap<>();
    private volatile LongToIntFunction partitionFunction;
    private volatile Supplier<RequestId> requestIdSupplier;
    private volatile CountDownLatch threads;
    private volatile String clientId;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile long maxRegistrationAgeInMillis;

    ResponseHandlers() {
        this.requestIdSupplier = requestIdSupplier;
    }

    @Override
    public String getName() {
        return "ResponseHandlers";
    }

    @Override
    protected void onStart(DbzNode node) {
        this.clientId = node.id();
        this.requestIdSupplier = () -> RequestId.create(clientId);
        // Create the partitions ...
        ClientConfiguration config = ClientConfiguration.adapt(node.getConfiguration());
        int numPartitions = config.getResponsePartitionCount();
        int maxBacklog = config.getMaxResponseBacklog();
        maxRegistrationAgeInMillis = config.getMaxResponseRegistrationAge(TimeUnit.MILLISECONDS);
        threads = new CountDownLatch(numPartitions);
        Sequences.times(numPartitions)
                 .mapToObj(i -> new Partition(maxBacklog, this::processResponse, this::partitionStarted, this::partitionStopped))
                 .forEach(partitions::add);
        // Run each of the partitions ...
        partitions.forEach(partition -> node.execute(partition.runnable));
        long slots = partitions.size() - 1;
        partitionFunction = input -> {
            return (int) (slots % input);
        };

        // Start the cleaner thread ..
        long delay = config.getCleanerDelay(TimeUnit.SECONDS);
        long period = config.getCleanerPeriod(TimeUnit.SECONDS);
        node.execute(delay, period, TimeUnit.SECONDS, this::cleanRegistrations);
    }

    @Override
    protected void beginShutdown(DbzNode node) {
        partitions.forEach(Partition::stop);
    }

    @Override
    protected void completeShutdown(DbzNode node) {
        if (threads != null) {
            try {
                drainRegistrations();
                threads.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
        }
    }

    protected void partitionStarted(Partition partition) {
    }

    protected void partitionStopped(Partition partition) {
        if (threads != null) threads.countDown();
    }

    /**
     * Register a new response handler.
     * 
     * @param context the context in which the handler operates; may not be null
     * @param parts the number of responses expected in the request; must be positive
     * @param handlers the response handlers; may not be null
     * @return an optional with the ID of the request if submitted, or an empty optional if this service is not running
     */
    public Optional<RequestId> register(ExecutionContext context, int parts, Handlers handlers) {
        assert context != null;
        assert handlers != null;
        assert parts > 0;
        return whenRunning(node -> {
            RequestId id = requestIdSupplier.get();
            try {
                lock.readLock().lock();
                return registrations.putIfAbsent(id, new Registration(context, parts, handlers)) != null ? null : id;
            } finally {
                lock.readLock().unlock();
            }
        });
    }

    /**
     * Register a new response handler.
     * 
     * @param context the context in which the handler operates; may not be null
     * @param parts the number of responses expected in the request; must be positive
     * @param successHandler the function to be called upon success of each part of the request; may be null
     * @param completionHandler the function to be called upon completion of the request; may be null
     * @param failureHandler the function to be called upon failure of the request; may be null
     * @return an optional with the ID of the request if submitted, or an empty optional if this service is not running
     */
    public Optional<RequestId> register(ExecutionContext context, int parts, Consumer<Document> successHandler, Callable completionHandler,
                                        BiConsumer<Outcome.Status, String> failureHandler) {
        return register(context, parts, with(successHandler, completionHandler, failureHandler));
    }

    /**
     * Submit a request and wait for the response.
     * 
     * @param context the context in which the handler operates; may not be null
     * @param timeout the number of seconds to wait for the response; must be positive
     * @param unit the time unit to wait for the response; may not be null
     * @param submitter the function that submits the request using the supplied request ID; may not be null
     * @param successHandler the function to be called upon success of each part of the request and which produces the result; may
     *            be null
     * @param failureHandler the function to be called upon failure of the request; may be null
     * @return the result of the {@code successHandler} function, or not present if the {@code failureHandler} was called
     */
    public <R> Optional<R> requestAndWait(ExecutionContext context, long timeout, TimeUnit unit, Consumer<RequestId> submitter,
                                          Function<Document, R> successHandler, BiConsumer<Outcome.Status, String> failureHandler) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<R> result = new AtomicReference<>();
        RequestId requestId = register(context, 1, with(doc -> result.set(successHandler.apply(doc)),
                                                        latch::countDown,
                                                        failureHandler)).orElseThrow(DebeziumClientException::new);
        // At this point, the handlers are registered, so any/all failures must be sent to the `failureHandler` ...
        try {
            submitter.accept(requestId);
            if (latch.await(timeout, unit)) {
                return Optional.ofNullable(result.get());
            }
            failureHandler.accept(Outcome.Status.TIMEOUT, "Timeout while waiting for results");
        } catch (InterruptedException e) {
            Thread.interrupted();
            failureHandler.accept(Outcome.Status.TIMEOUT, "Interrupted while waiting for results");
        } catch (RuntimeException e) {
            failureHandler.accept(Outcome.Status.COMMUNICATION_ERROR, e.getMessage());
        } finally {
            // No matter what, we need to remove the registration since we waited for this request to complete ...
            registrations.remove(requestId);
        }
        return Optional.empty();
    }

    /**
     * Submit a response message. This is typically called by the message consumer in the client that processes partial response
     * messages.
     * 
     * @param response the response; may not be null
     * @return {@code true} if successful, or {@code false} if the response could not be submitted because the service is
     *         no longer running or because there was not enough room in the service's queue
     */
    public boolean submit(Document response) {
        return submit(response, partition -> partition.submit(response));
    }

    /**
     * Submit a response and wait a duration of time if needed.
     * 
     * @param response the response; may not be null
     * @param timeout how long to wait before giving up, in units of {@code unit}
     * @param unit a {@code TimeUnit} determining how to interpret the {@code timeout} parameter
     * @return {@code true} if successful, or {@code false} if the specified waiting time elapses before space is available
     */
    public boolean submit(Document response, long timeout, TimeUnit unit) {
        return submit(response, partition -> {
            try {
                return partition.submit(response, timeout, unit);
            } catch (InterruptedException e) {
                Thread.interrupted();
                return false;
            }
        });
    }

    /**
     * Determine and find the {@link Partition} to which the request applies, and then call the supplied function with this
     * Partition.
     * @param response the partial response message from which the request ID is found; never null
     * @param submitFunction the function that should be called with the designated partition
     * @return the result of the {@code submitFunction}
     */
    private boolean submit(Document response, Function<Partition, Boolean> submitFunction) {
        assert response != null;
        return ifRunning(node -> {
            RequestId id = RequestId.from(response);
            if (!clientId.equals(id.getClientId())) return false;
            // Partition on the request number ...
            int index = partitionFunction.applyAsInt(id.getRequestNumber());
            Partition partition = partitions.get(index);
            return submitFunction.apply(partition);
        });
    }

    /**
     * Method to accept a new message from the 'partial-responses' topic, and to forward this to the handler registered
     * with the same request ID. Note that each request might produce multiple partial responses, so the registered handler is
     * removed only if this response message is the last partial response received for the request.
     * 
     * @param response the partial response document; never null
     */
    protected void processResponse(Document response) {
        logger.trace("Client is processing partial response message: \n{}", response);
        RequestId id = RequestId.from(response);
        // We don't need to lock for removal ...
        Registration registration = registrations.get(id);
        if (registration != null) {
            // Invoke the registered handler ...
            try {
                logger.trace("Calling registered handler for partial response with ID '{}' and message: \n{}", id, response);
                registration.handle(id, response, () -> registrations.remove(id));
            } catch (Throwable t) {
                logger().error("Unable to process response using handler {}: {}", registration, response, t);
            }
        }
    }

    /**
     * Looks at all registrations to see if they are expired. This method is called periodically from the cleaning thread.
     */
    protected void cleanRegistrations() {
        try {
            long now = System.currentTimeMillis();
            registrations.entrySet()
                         .stream()
                         .filter(entry -> entry.getValue().age(now) > maxRegistrationAgeInMillis)
                         .map(entry -> entry.getKey())
                         .collect(Collectors.toSet())
                         .forEach(this::expireRegistration);
        } catch (RuntimeException t) {
            logger().error("Error while cleaning expired response registrations", t);
        }
    }

    private void expireRegistration(RequestId requestId) {
        Registration registration = registrations.remove(requestId);
        if (registration != null) {
            try {
                registration.fail(Outcome.Status.TIMEOUT, "Registered callback expired and has been removed");
            } catch (RuntimeException e) {
                logger().error("Unable to process response using handler {}: {}", registration, e.getMessage(), e);
            }
        }
    }

    protected void drainRegistrations() {
        try {
            // Lock to prevent new registrations ...
            lock.writeLock().lock();
            try {
                registrations.values().forEach(registration -> {
                    try {
                        registration.fail(Outcome.Status.CLIENT_STOPPED, "Client stopped");
                    } catch (RuntimeException e) {
                        logger().error("Unable to process response using handler {}: {}", registration, e.getMessage(), e);
                    }
                });
            } finally {
                registrations.clear();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    protected boolean isEmpty() {
        return threads == null || threads.getCount() == 0;
    }
}
