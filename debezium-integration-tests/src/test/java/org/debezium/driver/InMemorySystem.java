/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import kafka.consumer.TopicFilter;
import kafka.producer.KeyedMessage;

import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.Serde;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.TaskCoordinator.RequestScope;
import org.apache.samza.task.WindowableTask;
import org.debezium.Testing;
import org.debezium.core.component.DatabaseId;
import org.debezium.core.doc.Document;
import org.debezium.core.message.Topic;
import org.debezium.core.serde.Decoder;
import org.debezium.core.serde.Encoder;
import org.debezium.core.serde.Serdes;
import org.debezium.samza.MemoryKeyValueStore;
import org.debezium.service.EntityBatchService;
import org.debezium.service.EntityStorageService;
import org.debezium.service.ResponseAccumulatorService;
import org.debezium.service.SchemaStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory representations of a Debezium.Client and all services using in-memory streams. This is designed to be used
 * within tests. Each topic only has a single partition.
 */
public class InMemorySystem implements Debezium.Client {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String systemName = "kafka";
    private final Decoder<String> keyDecoder = Serdes.stringDecoder();
    private final Decoder<Document> messageDecoder = Serdes.documentDecoder();
    private final Serde<String> keyEncoder = Serdes.string();
    private final Encoder<Document> messageEncoder = Serdes.documentEncoder();

    private final Debezium.Client client;
    private final InMemoryAsyncFoundation foundation = new InMemoryAsyncFoundation("kafka", null, this::getExecutor);
    private final Environment env = Environment.create(exec -> foundation);
    private final List<StreamTask> services = new ArrayList<>();
    private final List<Runnable> windowables = new ArrayList<>();
    private final MetricsRegistry metricsRegistry = null;
    private final ConcurrentMap<String, KeyValueStore<String, Document>> storesByName = new ConcurrentHashMap<>();

    public InMemorySystem() {
        // Wire up the services to the foundation, which uses a separate thread for each service ...
        Config serviceConfig = new MapConfig();
        addService(new EntityBatchService(), serviceConfig, "entity-batch-service", Topic.ENTITY_BATCHES);
        addService(new EntityStorageService(), serviceConfig, "entity-storage-service", Topic.ENTITY_PATCHES);
        addService(new ResponseAccumulatorService(), serviceConfig, "schema-storage-service", Topic.PARTIAL_RESPONSES);
        addService(new SchemaStorageService(), serviceConfig, "response-accumulator-service", Topic.SCHEMA_PATCHES);

        // Create the client ...
        Configuration config = Debezium.configure().build();
        client = Debezium.start(config, env);
    }

    /**
     * Signal that the {@link WindowableTask#window(MessageCollector, TaskCoordinator)} method be called (asynchronously) on all
     * {@link WindowableTask windowable service}.
     */
    public void fireWindow() {
        windowables.forEach(Runnable::run);
    }

    @Override
    public Database connect(DatabaseId id, String username, String device, String appVersion) {
        return client.connect(id, username, device, appVersion);
    }
    
    @Override
    public Database connect(DatabaseId id, String username, String device, String appVersion, long timeout, TimeUnit unit) {
        return client.connect(id, username, device, appVersion, timeout, unit);
    }

    @Override
    public Database provision(DatabaseId id, String username, String device, String appVersion) {
        return client.provision(id, username, device, appVersion);
    }
    
    @Override
    public Database provision(DatabaseId id, String username, String device, String appVersion, long timeout, TimeUnit unit) {
        return client.provision(id, username, device, appVersion,timeout,unit);
    }

    @Override
    public void shutdown(long timeout, TimeUnit unit) {
        try {
            client.shutdown(timeout, unit);
        } finally {
            foundation.shutdown();
        }
    }

    private Executor getExecutor() {
        return env.getExecutor();
    }

    protected void addService(StreamTask service, String serviceName, String... inputTopics) {
        addService(service, null, serviceName, inputTopics);
    }

    protected void addService(StreamTask service, Config config, String serviceName, String... inputTopics) {
        logger.debug("SYSTEM: adding service '{}' to topics {}",serviceName,inputTopics);
        
        // Create a message queue that accumulates messages ...
        MessageQueue messageQueue = createMessageQueue(serviceName);

        // Set up a consumer that will call the service ...
        MessageConsumer<String, Document> consumer = (topic, partition, offset, key, message) -> {
            // Create the envelope for the input message ...
            SystemStreamPartition stream = new SystemStreamPartition(systemName, topic, new Partition(partition));
            String offsetStr = Integer.toString(partition);
            IncomingMessageEnvelope inputEnvelope = new IncomingMessageEnvelope(stream, offsetStr, key, message);

            try {
                if (messageQueue.isWindowToBeFired()) {
                    // It's time to call the window method on the service ...
                    try {
                        logger.debug("SERVICE {}: Invoking window()",service.getClass().getSimpleName());
                        ((WindowableTask) service).window(messageQueue.collector(), messageQueue.coordinator());
                        messageQueue.sendAll();
                    } catch (Exception e) {
                        Testing.printError(e);
                    }
                }
                // Call the service ...
                logger.debug("SERVICE {}: Processing message: {}",service.getClass().getSimpleName(),message);
                service.process(inputEnvelope, messageQueue.collector(), messageQueue.coordinator());
                // and send all accumulated messages ...
                messageQueue.sendAll();
                return true;
            } catch (Exception e) {
                Testing.printError(e);
                return false;
            }
        };

        // Initialize the service ...
        if (service instanceof InitableTask) {
            // Create a task context ...
            InitableTask itask = (InitableTask) service;
            try {
                logger.debug("SERVICE {}: Initializing",service.getClass().getSimpleName());
                itask.init(config, taskContext(serviceName));
            } catch (Exception e) {
                Testing.printError(e);
            }
        }

        if (service instanceof WindowableTask) {
            windowables.add(messageQueue::fireWindow);
        }

        // Ensure that the topics exist ...
        foundation.createStreams(inputTopics);

        // Add the service as a consumer for each of the topics ...
        TopicFilter topicFilter = Topics.anyOf(inputTopics);
        foundation.subscribe(serviceName, topicFilter, 1, keyDecoder, messageDecoder, consumer); // will start running immediately

        // Record and initialize the service ...
        services.add(service);
    }

    protected KeyValueStore<String, Document> getOrCreateStore(String name) {
        return storesByName.compute(name, (storeName, store) -> store != null ? store : newStore(storeName));
    }

    protected KeyValueStore<String, Document> newStore(String name) {
        return new MemoryKeyValueStore<String, Document>(name);
    }

    protected TaskContext taskContext(String topicName) {
        return new TaskContext() {
            @Override
            public TaskName getTaskName() {
                return new TaskName(topicName);
            }

            @Override
            public Set<SystemStreamPartition> getSystemStreamPartitions() {
                return foundation.getSystemStreamPartitions();
            }

            @Override
            public void setStartingOffset(SystemStreamPartition ssp, String offset) {
            }

            @Override
            public MetricsRegistry getMetricsRegistry() {
                return metricsRegistry;
            }

            @Override
            public Object getStore(String name) {
                return getOrCreateStore(name);
            }
        };
    }

    protected MessageQueue createMessageQueue(String name) {
        return createMessageQueue(name,null, null);
    }

    protected MessageQueue createMessageQueue(String name, Consumer<RequestScope> commitFunction) {
        return createMessageQueue(name,commitFunction, null);
    }

    protected MessageQueue createMessageQueue(String name, Consumer<RequestScope> commitFunction, Consumer<RequestScope> shutdownFunction) {
        // Create a message collector that will accumulate the messages until the coordinator is committed ...
        Queue<KeyedMessage<byte[], byte[]>> messages = new LinkedList<>();
        MessageCollector messageCollector = new MessageCollector() {
            @Override
            public void send(OutgoingMessageEnvelope envelope) {
                String topic = envelope.getSystemStream().getStream();
                Object partitionKey = envelope.getPartitionKey();
                byte[] key = keyEncoder.toBytes((String) envelope.getKey());
                byte[] message = messageEncoder.toBytes((Document) envelope.getMessage());
                KeyedMessage<byte[], byte[]> rawMessage = new KeyedMessage<>(topic, key, partitionKey, message);
                messages.add(rawMessage);
                if ( logger.isTraceEnabled() ) {
                    logger.trace("TOPIC: add message with key '{}' and value: {}",topic,envelope.getMessage());
                }
            }
        };

        // Create a task coordinator ...
        TaskCoordinator taskCoordinator = new TaskCoordinator() {
            @Override
            public void commit(RequestScope scope) {
                if (commitFunction != null) commitFunction.accept(scope);
            }

            @Override
            public void shutdown(RequestScope scope) {
                if (shutdownFunction != null) shutdownFunction.accept(scope);
            }
        };

        return new MessageQueue(name,messages, messageCollector, taskCoordinator, foundation::producer);
    }

    protected static final class MessageQueue {
        private final String name;
        private final Logger logger = LoggerFactory.getLogger(getClass());
        private final MessageCollector messageCollector;
        private final TaskCoordinator taskCoordinator;
        private final Queue<KeyedMessage<byte[], byte[]>> messages;
        private final Supplier<MessageProducer> producer;
        private final AtomicBoolean window = new AtomicBoolean(false);

        protected MessageQueue(String name, Queue<KeyedMessage<byte[], byte[]>> messages, MessageCollector messageCollector,
                TaskCoordinator taskCoordinator, Supplier<MessageProducer> producer) {
            this.name = name;
            this.messages = messages;
            this.messageCollector = messageCollector;
            this.taskCoordinator = taskCoordinator;
            this.producer = producer;
        }

        public MessageCollector collector() {
            return messageCollector;
        }

        public TaskCoordinator coordinator() {
            return taskCoordinator;
        }

        public void sendAll() {
            if ( logger.isDebugEnabled() && messages.isEmpty() ) {
                logger.debug("QUEUE {}: no messages to send",name);
            }
            while (!messages.isEmpty()) {
                KeyedMessage<byte[], byte[]> message = messages.remove();
                if ( logger.isTraceEnabled() ) {
                    logger.trace("QUEUE {}: send message with key '{}' on topic '{}'",name,new String(message.key()),message.topic());
                }
                producer.get().send(message);
            }
        }

        public boolean isWindowToBeFired() {
            return window.getAndSet(false);
        }

        public void fireWindow() {
            window.set(true);
        }
    }
}
