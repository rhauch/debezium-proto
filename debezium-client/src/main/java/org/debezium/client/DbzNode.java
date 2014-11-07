/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.client;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
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
import java.util.function.Function;
import java.util.function.Supplier;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import kafka.serializer.StringEncoder;
import kafka.utils.VerifiableProperties;

import org.debezium.core.doc.Document;
import org.debezium.core.doc.DocumentReader;
import org.debezium.core.doc.DocumentWriter;
import org.debezium.core.doc.Value;
import org.debezium.core.util.LazyReference;
import org.debezium.core.util.Strings;

/**
 * A component that represents a single compute node within the Debezium cluster.
 * 
 * @author Randall Hauch
 */
final class DbzNode {

    /**
     * A function that decodes a byte array into another type.
     * 
     * @param <T> the type of object to decode
     */
    @FunctionalInterface
    public static interface Decoder<T> {
        /**
         * Decode the byte array into an object.
         * 
         * @param bytes the bytes; never null
         * @return the object; never null
         */
        T fromBytes(byte[] bytes);
    }

    /**
     * A function that encodes an object into a byte array.
     * 
     * @param <T> the type of object to encode
     */
    @FunctionalInterface
    public static interface Encoder<T> {
        /**
         * Encode the object into a byte array.
         * 
         * @param value the value to encode; never null
         * @return the bytes; never null
         */
        byte[] toBytes(T value);
    }

    /**
     * A function that consumes a message from a topic.
     *
     * @param <KeyType> the type of key
     * @param <MessageType> the type of message
     */
    @FunctionalInterface
    public static interface MessageConsumer<KeyType, MessageType> {
        /**
         * Consume a message at the given offset in the given partition from the named topic.
         * 
         * @param topic the name of the topic
         * @param partition the partition number
         * @param offset the logical offset within the partition
         * @param key the key for the message
         * @param message the message
         * @return {@code true} if the message was fully consumed, or {@code false} if the message was not fully consumed
         */
        boolean consume(String topic, int partition, long offset, KeyType key, MessageType message);
    }

    /**
     * A component that can run within a {@link DbzNode}.
     */
    public static abstract class Service {

        private final ReadWriteLock startLock = new ReentrantReadWriteLock();
        private final AtomicReference<DbzNode> node = new AtomicReference<>();

        /**
         * Start the service running within the given {@link DbzNode}.
         * 
         * @param node the node in which the service is to run; never null
         */
        public final void start(DbzNode node) {
            try {
                this.startLock.writeLock().lock();
                if (this.node.get() == null) {
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
            return node.get().logger(getClass());
        }
    }

    private static final DefaultDecoder DEFAULT_DECODER = new DefaultDecoder(new VerifiableProperties());
    private static final StringDecoder STRING_RAW_DECODER = new StringDecoder(new VerifiableProperties());
    private static final StringEncoder STRING_RAW_ENCODER = new StringEncoder(new VerifiableProperties());

    private static final Decoder<String> STRING_DECODER = new Decoder<String>() {
        @Override
        public String fromBytes(byte[] bytes) {
            return STRING_RAW_DECODER.fromBytes(bytes);
        }
    };

    private static final DocumentWriter DOCUMENT_WRITER = DocumentWriter.defaultWriter();
    private static final DocumentReader DOCUMENT_READER = DocumentReader.defaultReader();

    private static final Decoder<Document> DOCUMENT_DECODER = new Decoder<Document>() {
        @Override
        public Document fromBytes(byte[] bytes) {
            try {
                return DOCUMENT_READER.read(bytes);
            } catch (IOException e) {
                // Should never see this, but shit if we do ...
                throw new RuntimeException(e);
            }
        }
    };

    @FunctionalInterface
    public static interface Callable {
        void call();
    }

    private final String nodeId = UUID.randomUUID().toString();
    private final Properties producerConfig;
    private final Properties consumerConfig;
    private final Document config;
    private final Supplier<Executor> executor;
    private final Supplier<ScheduledExecutorService> scheduledExecutor;
    private final List<ScheduledFuture<?>> scheduledTasks = new CopyOnWriteArrayList<>();
    private final LazyReference<Producer<byte[], byte[]>> producer;
    private final CopyOnWriteArrayList<Service> services = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Callable> preShutdownListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Callable> postShutdownListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Callable> startListeners = new CopyOnWriteArrayList<>();
    private final Lock runningLock = new ReentrantLock();
    private volatile boolean running = false;

    public DbzNode(Document config, Supplier<Executor> executor, Supplier<ScheduledExecutorService> scheduledExecutor) {
        this.config = config;
        this.producerConfig = DbzConfiguration.asProperties(config.getDocument(DbzConfiguration.PRODUCER_SECTION));
        this.consumerConfig = DbzConfiguration.asProperties(config.getDocument(DbzConfiguration.CONSUMER_SECTION));
        this.executor = executor;
        this.scheduledExecutor = scheduledExecutor;
        this.producer = LazyReference.create(this::createProducer);
        // On node startup, start all registered services ...
        registerStart(() -> services.forEach((service) -> service.start(this)));
        // On node shutdown, stop all registered services ...
        registerPreShutdown(() -> services.forEach(Service::beginShutdown));
        registerPostShutdown(() -> services.forEach(Service::completeShutdown));
    }
    
    private Producer<byte[],byte[]> createProducer() {
        ProducerConfig pconf = new ProducerConfig(this.producerConfig);
        return new Producer<byte[], byte[]>(pconf);
    }

    /**
     * Get the configuration property with the given name.
     * 
     * @param propertyName the name of the configuration property
     * @param defaultValue the default value for the property; may be null or a {@link Value#nullValue() null value}
     * @return the configuration's value, or the {@code defaultValue} if the configuration does not contain the configuration
     *         property
     */
    public Value getConfig(String propertyName, Value defaultValue) {
        return config.get(propertyName, defaultValue);
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
        if (notified != null) this.startListeners.addIfAbsent(notified);
    }

    /**
     * Register a function that is to be invoked at the beginning of any subsequent call to {@link #shutdown()}.
     * This method does nothing if {@code notified} is null.
     * 
     * @param notified the function to be invoked
     */
    public void registerPreShutdown(Callable notified) {
        if (notified != null) this.preShutdownListeners.addIfAbsent(notified);
    }

    /**
     * Register a function that is to be invoked at the end of any subsequent call to {@link #shutdown()}.
     * This method does nothing if {@code notified} is null.
     * 
     * @param notified the function to be invoked
     */
    public void registerPostShutdown(Callable notified) {
        if (notified != null) this.postShutdownListeners.addIfAbsent(notified);
    }

    /**
     * Add one or more services that will each be automatically {@link Service#start(DbzNode) started} when this node is
     * {@link #start() started} and automatically {@link Service#beginShutdown() stopped} when
     * this node is {@link #shutdown() shutdown}.
     * 
     * @param services one or more services to add
     */
    public void add(Service... services) {
        Arrays.stream(services).forEach((service) -> {
            if (service != null && this.services.addIfAbsent(service)) {
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
        Arrays.stream(services).forEach((service) -> {
            if (service != null && this.services.remove(service)) {
                whenNotRunning(() -> {
                    service.beginShutdown();
                    service.completeShutdown();
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
            if ( !this.config.get(DbzConfiguration.INIT_PRODUCER_LAZILY,false).convert().asBoolean() ) {
                this.producer.get();
            }
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
                    // Shutdown the producer if it was accessed ...
                    producer.release(Producer::close);
                } finally {
                    // Cancel all of the scheduled tasks ...
                    try {
                        scheduledTasks.forEach(f->f.cancel(true));
                    } finally {
                        scheduledTasks.clear();
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
        if (producer.get() != null) {
            producer.get().send(new KeyedMessage<>(topic, key, partitionKey, msg));
            return true;
        }
        return false;
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
        return send(topic, partitionKey, STRING_RAW_ENCODER.toBytes(key), msg);
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
        return send(topic, partitionKey, STRING_RAW_ENCODER.toBytes(key), DOCUMENT_WRITER.writeAsBytes(doc));
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
        return send(topic, STRING_RAW_ENCODER.toBytes(key), msg);
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
        return send(topic, STRING_RAW_ENCODER.toBytes(key), DOCUMENT_WRITER.writeAsBytes(doc));
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
        subscribe(groupId, topicFilter, numThreads, STRING_DECODER, messageDecoder, consumer);
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
        subscribe(groupId, topicFilter, numThreads, STRING_DECODER, DOCUMENT_DECODER, consumer);
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
        // Create the config for this consumer ...
        boolean autoCommit = false;
        Properties props = new Properties();
        props.putAll(this.consumerConfig);
        props.put("consumer.id", id());
        props.put("group.id", groupId);
        props.put("auto.commit.enable", Boolean.toString(autoCommit));

        // Create the consumer ...
        ConsumerConfig config = new ConsumerConfig(props);
        ConsumerConnector connector = kafka.consumer.Consumer.createJavaConsumerConnector(config);
        List<KafkaStream<byte[], byte[]>> streams = connector.createMessageStreamsByFilter(topicFilter, numThreads, DEFAULT_DECODER,
                                                                                           DEFAULT_DECODER);

        // Iterate over the streams and create a thread to process each one ...
        for (KafkaStream<byte[], byte[]> stream : streams) {
            // Submit a runnable that consumes the topics ...
            this.executor.get().execute(() -> {
                ConsumerIterator<byte[], byte[]> iter = stream.iterator();
                boolean success = false;
                while (running) {
                    try {
                        while (running && iter.hasNext()) {
                            MessageAndMetadata<byte[], byte[]> msg = iter.next();
                            success = consumer.consume(msg.topic(), msg.partition(), msg.offset(), keyDecoder.fromBytes(msg.key()),
                                                       messageDecoder.fromBytes(msg.message()));
                            if (success && autoCommit) {
                                connector.commitOffsets();
                            }
                        }
                    } catch (ConsumerTimeoutException e) {
                        // Keep going ...
                    }
                }
            });
        }
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
            this.scheduledTasks.add(this.scheduledExecutor.get().scheduleAtFixedRate(()->runnable.call(), initialDelay, period, unit));
        }
    }

    /**
     * Get a logger for the context with the given classname, where all log messages are sent to the "log" topic.
     * 
     * @param clazz the class representing the context
     * @return the logger; never null
     */
    public Logger logger(Class<?> clazz) {
        return logger(clazz.getName());
    }

    /**
     * Get a logger for the context with the given classname, where all log messages are sent to the "log" topic.
     * 
     * @param classname the classname representing the context
     * @return the logger; never null
     */
    public Logger logger(String classname) {
        return new Logger() {
            @Override
            public void log(Level level, String msg, Object... params) {
                DbzNode.this.log(classname, level, null, msg, params);
            }

            @Override
            public void log(Level level, Throwable error, String msg, Object... params) {
                DbzNode.this.log(classname, level, error, msg, params);
            }
        };
    }

    private void log(String classname, Logger.Level level, Throwable t, String msg, Object[] params) {
        Document doc = Document.create();
        doc.setString("level", level.toString());
        doc.setString("msg", msg);
        if (t != null) {
            doc.setString("error", t.getMessage());
            doc.setString("stackTrace", Strings.getStackTrace(t));
        }
        doc.setNumber("timestamp", System.currentTimeMillis());
        doc.set("name", classname);
        if (params != null) {
            for (int i = 0; i != params.length; ++i) {
                Object param = params[i];
                String paramName = "$" + i;
                doc.set(paramName, param);
            }
        }
        send("log", "", doc);
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public String toString() {
        return id() + " (" + (running ? "running" : "stopped") + ")";
    }

}
