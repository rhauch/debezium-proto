/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import kafka.consumer.TopicFilter;
import kafka.producer.KeyedMessage;

import org.debezium.core.doc.Document;
import org.debezium.core.function.Callable;
import org.debezium.core.function.Predicates;
import org.debezium.core.serde.Decoder;
import org.debezium.core.serde.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A component that represents a single compute node within the Debezium cluster.
 * 
 * @author Randall Hauch
 */
final class DbzNode {

    /**
     * A component that can run within a {@link DbzNode}.
     */
    public static abstract class Service {

        private final ReadWriteLock startLock = new ReentrantReadWriteLock();
        private final AtomicReference<DbzNode> node = new AtomicReference<>();
        private Logger logger = null;
        
        public abstract String getName();

        /**
         * Start the service running within the given {@link DbzNode}.
         * 
         * @param node the node in which the service is to run; never null
         */
        public final void start(DbzNode node) {
            try {
                this.startLock.writeLock().lock();
                if (this.node.get() == null) {
                    this.logger = DbzNode.logger(getClass());
                    onStart(node);
                    this.node.set(node); // do this *after* calling 'onStart', in case the call fails
                }
            } finally {
                this.startLock.writeLock().unlock();
            }
        }

        protected abstract void onStart(DbzNode node);

        protected abstract void beginShutdown(DbzNode node);

        protected abstract void completeShutdown(DbzNode node);

        protected <R> Optional<R> whenRunning(Function<DbzNode, R> function) {
            try {
                this.startLock.readLock().lock();
                DbzNode node = this.node.get();
                if (node != null) {
                    return Optional.ofNullable(function.apply(node));
                }
                return Optional.empty();
            } finally {
                this.startLock.readLock().unlock();
            }
        }

        protected <R> boolean ifRunning(Function<DbzNode, Boolean> function) {
            try {
                this.startLock.readLock().lock();
                DbzNode node = this.node.get();
                if (node != null) {
                    return function.apply(node);
                }
                return false;
            } finally {
                this.startLock.readLock().unlock();
            }
        }

        /**
         * Begin to shut down the service.
         */
        public final void beginShutdown() {
            try {
                this.startLock.readLock().lock();
                beginShutdown(this.node.get());
            } finally {
                this.startLock.readLock().unlock();
            }
        }

        /**
         * Complete the service shut down.
         */
        public final void completeShutdown() {
            try {
                this.startLock.writeLock().lock();
                completeShutdown(this.node.get());
                this.node.set(null);
            } finally {
                this.startLock.writeLock().unlock();
            }
        }

        protected Logger logger() {
            return logger;
        }
    }

    private final String nodeId = UUID.randomUUID().toString();
    private final Configuration config;
    private final Supplier<MessageBus> messageBus;
    private final Supplier<Executor> executor;
    private final Supplier<ScheduledExecutorService> scheduledExecutor;
    private final List<ScheduledFuture<?>> scheduledTasks = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Service> services = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Callable> preShutdownListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Callable> postShutdownListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Callable> startListeners = new CopyOnWriteArrayList<>();
    private final Lock runningLock = new ReentrantLock();
    private final Logger logger = DbzNode.logger(getClass());
    private volatile boolean running = false;

    DbzNode(Configuration config, Environment env ) {
        this.config = config;
        this.messageBus = env::getMessageBus;
        this.executor = env::getExecutor;
        this.scheduledExecutor = env::getScheduledExecutor;
        // On node startup, start all registered services ...
        registerStart(() -> services.forEach((service) -> service.start(this)));
        // On node shutdown, stop all registered services ...
        registerPreShutdown(() -> services.forEach(Service::beginShutdown));
        registerPostShutdown(() -> services.forEach(Service::completeShutdown));
    }

    /**
     * Get the configuration.
     * 
     * @return the configuration; never null
     */
    public Configuration getConfiguration() {
        return config;
    }

    /**
     * Get the unique identifier of this node.
     * 
     * @return this node's identifier; never null and never empty
     */
    public String id() {
        return nodeId;
    }

    /**
     * Register a function that is to be invoked when {@link #start()} is subsequently called.
     * This method does nothing if {@code notified} is null.
     * 
     * @param notified the function to be invoked
     */
    public void registerStart(Callable notified) {
        if (notified != null && this.startListeners.addIfAbsent(notified)) {
            logger.trace("Registering start listener: {}", notified);
        }
    }

    /**
     * Register a function that is to be invoked at the beginning of any subsequent call to {@link #shutdown()}.
     * This method does nothing if {@code notified} is null.
     * 
     * @param notified the function to be invoked
     */
    public void registerPreShutdown(Callable notified) {
        if (notified != null && this.preShutdownListeners.addIfAbsent(notified)) {
            logger.trace("Registering pre-shutdown listener: {}", notified);
        }
    }

    /**
     * Register a function that is to be invoked at the end of any subsequent call to {@link #shutdown()}.
     * This method does nothing if {@code notified} is null.
     * 
     * @param notified the function to be invoked
     */
    public void registerPostShutdown(Callable notified) {
        if (notified != null && this.postShutdownListeners.addIfAbsent(notified)) {
            logger.trace("Registering post-shutdown listener: {}", notified);
        }
    }

    /**
     * Add one or more services that will each be automatically {@link Service#start(DbzNode) started} when this node is
     * {@link #start() started} and automatically {@link Service#beginShutdown() stopped} when
     * this node is {@link #shutdown() shutdown}.
     * 
     * @param services one or more services to add
     */
    public void add(Service... services) {
        Arrays.stream(services)
              .filter(Predicates.notNull())
              .forEach(service -> {
                  if (this.services.addIfAbsent(service)) {
                      logger.debug("Added service {}", service.getName());
                      whenRunning(() -> service.start(this));
                  }
              });
    }

    /**
     * Remove one or more a services. Each service that was previously {@link #add(Service...) added} will be automatically
     * stopped.
     * 
     * @param services one or more services to remove
     */
    public void remove(Service... services) {
        Arrays.stream(services)
              .filter(Predicates.notNull())
              .forEach(service -> {
                  if (this.services.remove(service)) {
                      logger.debug("Removed service {}", service);
                      whenNotRunning(() -> {
                          logger.debug("Beginning shutdown of service {}", service);
                          service.beginShutdown();
                          service.completeShutdown();
                          logger.debug("Completed shutdown of service {}", service);
                      });
                  }
              });
    }

    /**
     * Call the supplied function if and only if this node is running.
     * 
     * @param function the function; may not be null
     * @return true if the function was called and this service is running, or false if the function was not called because the
     *         service is not running
     */
    public boolean whenRunning(Callable function) {
        try {
            runningLock.lock();
            if (running) {
                function.call();
                return true;
            }
            return false;
        } finally {
            runningLock.unlock();
        }
    }

    /**
     * Call the supplied function that returns a value if and only if this node is running.
     * 
     * @param function the function; may not be null
     * @return an optional with the result of the function, or empty if the node was not running
     */
    public <R> Optional<R> whenRunning(Supplier<R> function) {
        try {
            runningLock.lock();
            if (running) {
                return Optional.ofNullable(function.get());
            }
            return Optional.empty();
        } finally {
            runningLock.unlock();
        }
    }

    /**
     * Call the supplied function that returns a value if and only if this node is running.
     * 
     * @param function the function; may not be null
     * @return {@code false} if the node was not running, or the result of the function if running
     */
    protected boolean ifRunning(BooleanSupplier function) {
        try {
            runningLock.lock();
            if (running) {
                return function.getAsBoolean();
            }
            return false;
        } finally {
            runningLock.unlock();
        }
    }

    /**
     * Call the supplied function if and only if this node is not running.
     * 
     * @param function the function; may not be null
     */
    public void whenNotRunning(Callable function) {
        try {
            runningLock.lock();
            if (!running) function.call();
        } finally {
            runningLock.unlock();
        }
    }

    /**
     * Start this node. This will also notify each of the {@link #registerStart(Callable) registered startup} callbacks.
     */
    public void start() {
        whenNotRunning(() -> {
            this.running = true;
            this.startListeners.forEach(Callable::call);
        });
    }

    /**
     * Shut down this node and release any reserved resources. This will also notify each of the
     * {@link #registerPreShutdown(Callable) registered pre-shutdown} callbacks
     * and the {@link #registerPostShutdown(Callable) registered post-shutdown} callbacks.
     */
    public void shutdown() {
        whenRunning(() -> {
            // Mark as not running ...
            this.running = false;
            try {
                preShutdownListeners.forEach(Callable::call);
            } finally {
                try {
                    // Cancel all of the scheduled tasks ...
                    scheduledTasks.forEach(f -> f.cancel(true));
                } finally {
                    scheduledTasks.clear();
                    try {
                        // Shutdown the producer if it was accessed ...
                        messageBus.get().shutdown();
                    } finally {
                        postShutdownListeners.forEach(Callable::call);
                    }
                }
            }
        });
    }

    /**
     * Send the binary array as a message on the named topic using the given key for the message and a different key that will
     * be used for partitioning.
     * 
     * @param topic the name of the topic; may not be null
     * @param partitionKey the key that will be used to determine the partition, but it will not be stored in the message;
     *            may be null if the {@code key} should be used for partitioning
     * @param key the key for the message; may not be null
     * @param msg the message; may not be null
     * @return {@code true} if message was sent, or {@code false} otherwise
     */
    public boolean send(String topic, Object partitionKey, byte[] key, byte[] msg) {
        return messageBus.get().producer().send(new KeyedMessage<>(topic, key, partitionKey, msg));
    }

    /**
     * Send the binary array as a message on the named topic using the given key for the message.
     * 
     * @param topic the name of the topic; may not be null
     * @param key the key for the message; may not be null
     * @param msg the message; may not be null
     * @return {@code true} if message was sent, or {@code false} otherwise
     */
    public boolean send(String topic, byte[] key, byte[] msg) {
        return send(topic, null, key, msg);
    }

    /**
     * Send the binary array as a message on the named topic using the given key for the message and a different key that will
     * be used for partitioning.
     * 
     * @param topic the name of the topic; may not be null
     * @param partitionKey the key that will be used to determine the partition, but it will not be stored in the message;
     *            may be null if the {@code key} should be used for partitioning
     * @param key the key for the message; may not be null
     * @param msg the message; may not be null
     * @return {@code true} if message was sent, or {@code false} otherwise
     */
    public boolean send(String topic, Object partitionKey, String key, byte[] msg) {
        return send(topic, partitionKey, Serdes.stringToBytes(key), msg);
    }

    /**
     * Send the document as a message on the named topic using the given key for the message and a different key that will
     * be used for partitioning.
     * 
     * @param topic the name of the topic; may not be null
     * @param partitionKey the key that will be used to determine the partition, but it will not be stored in the message;
     *            may be null if the {@code key} should be used for partitioning
     * @param key the key for the message; may not be null
     * @param doc the message document; may not be null
     * @return {@code true} if message was sent, or {@code false} otherwise
     */
    public boolean send(String topic, Object partitionKey, String key, Document doc) {
        return send(topic, partitionKey, Serdes.stringToBytes(key), Serdes.documentToBytes(doc));
    }

    /**
     * Send the binary array as a message on the named topic using the given key for the message.
     * 
     * @param topic the name of the topic; may not be null
     * @param key the key for the message; may not be null
     * @param msg the message; may not be null
     * @return {@code true} if message was sent, or {@code false} otherwise
     */
    public boolean send(String topic, String key, byte[] msg) {
        return send(topic, Serdes.stringToBytes(key), msg);
    }

    /**
     * Send the document as a message on the named topic using the given key for the message.
     * 
     * @param topic the name of the topic; may not be null
     * @param key the key for the message; may not be null
     * @param doc the message document; may not be null
     * @return {@code true} if message was sent, or {@code false} otherwise
     */
    public boolean send(String topic, String key, Document doc) {
        return send(topic, Serdes.stringToBytes(key), Serdes.documentToBytes(doc));
    }

    /**
     * Subscribe to one or more topics.
     * 
     * @param groupId the identifier of the consumer's group; may not be null
     * @param topicFilter the filter for the topics; may not be null
     * @param numThreads the number of threads on which consumers should be called
     * @param messageDecoder the decoder that should be used to convert the {@code byte[]} message into an object form expected by
     *            the consumer
     * @param consumer the consumer, which should be threadsafe if {@code numThreads} is more than 1
     */
    public <MessageType> void subscribe(String groupId, TopicFilter topicFilter, int numThreads, Decoder<MessageType> messageDecoder,
                                        MessageConsumer<String, MessageType> consumer) {
        subscribe(groupId, topicFilter, numThreads, Serdes::bytesToString, messageDecoder, consumer);
    }

    /**
     * Subscribe to one or more topics.
     * 
     * @param groupId the identifier of the consumer's group; may not be null
     * @param topicFilter the filter for the topics; may not be null
     * @param numThreads the number of threads on which consumers should be called
     * @param consumer the consumer, which should be threadsafe if {@code numThreads} is more than 1
     */
    public void subscribe(String groupId, TopicFilter topicFilter, int numThreads, MessageConsumer<String, Document> consumer) {
        subscribe(groupId, topicFilter, numThreads, Serdes::bytesToString, Serdes::bytesToDocument, consumer);
    }

    /**
     * Subscribe to one or more topics.
     * 
     * @param groupId the identifier of the consumer's group; may not be null
     * @param topicFilter the filter for the topics; may not be null
     * @param numThreads the number of threads on which consumers should be called
     * @param keyDecoder the decoder that should be used to convert the {@code byte[]} key into an object form expected by the
     *            consumer
     * @param messageDecoder the decoder that should be used to convert the {@code byte[]} message into an object form expected by
     *            the consumer
     * @param consumer the consumer, which should be threadsafe if {@code numThreads} is more than 1
     */
    public <KeyType, MessageType> void subscribe(String groupId, TopicFilter topicFilter, int numThreads, Decoder<KeyType> keyDecoder,
                                                 Decoder<MessageType> messageDecoder, MessageConsumer<KeyType, MessageType> consumer) {
        logger.debug("NODE: subscribing {} in group '{}' to topics {}",consumer,groupId,topicFilter);
        messageBus.get().subscribe(groupId, topicFilter, numThreads, keyDecoder, messageDecoder, consumer);
    }
    
    /**
     * Call the supplied runnable using this node's {@link Executor executor}.
     * 
     * @param runnable the runnable function; never null
     */
    public void execute(Runnable runnable) {
        if (runnable != null) this.executor.get().execute(runnable);
    }

    /**
     * Periodically call the supplied runnable using this node's {@link Executor executor}. The thread will terminate
     * automatically when this service is stopped.
     * 
     * @param initialDelay the initial delay before the function is first called
     * @param period the time between calls
     * @param unit the time unit; may not be null
     * @param runnable the runnable function; never null
     */
    public void execute(long initialDelay, long period, TimeUnit unit, Callable runnable) {
        if (runnable != null) {
            ScheduledFuture<?> future = this.scheduledExecutor.get().scheduleAtFixedRate(() -> runnable.call(), initialDelay, period, unit);
            this.scheduledTasks.add(future);
        }
    }

    /**
     * Get a logger for the context with the given classname, where all log messages are sent to the "log" topic.
     * 
     * @param clazz the class representing the context
     * @return the logger; never null
     */
    public static Logger logger(Class<?> clazz) {
        return LoggerFactory.getLogger(clazz.getName());
    }

    /**
     * Get a logger for the context with the given classname, where all log messages are sent to the "log" topic.
     * 
     * @param name the name representing the context
     * @return the logger; never null
     */
    public static Logger logger(String name) {
        return LoggerFactory.getLogger(name);
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public String toString() {
        return id() + " (" + (running ? "running" : "stopped") + ")";
    }

}
