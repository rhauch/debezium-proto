/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import static org.debezium.assertions.DebeziumAssertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorTopologyTest.CustomTimestampExtractor;
import org.apache.kafka.test.ProcessorTopologyTestDriver;
import org.debezium.Configuration;
import org.debezium.Testing;
import org.debezium.assertions.MessageAssert;
import org.debezium.kafka.Serdes;
import org.debezium.message.Array;
import org.debezium.message.Document;
import org.debezium.message.DocumentSerdes;
import org.debezium.message.Message;
import org.debezium.message.Patch;
import org.debezium.message.Records;
import org.debezium.model.DatabaseId;
import org.debezium.model.Entity;
import org.debezium.model.EntityId;
import org.debezium.model.EntityType;
import org.debezium.model.Identifier;
import org.debezium.util.RandomContent;
import org.debezium.util.RandomContent.ContentGenerator;
import org.debezium.util.RandomContentTest;
import org.junit.After;
import org.junit.Before;

import static org.fest.assertions.Assertions.assertThat;

/**
 * A base class for test cases that verify the functionality of a service's {@link Processor} implementation.
 * 
 * @author Randall Hauch
 */
public abstract class TopologyTest implements Testing {

    protected static final RandomContent CONTENT = RandomContent.load("load-data.txt", RandomContentTest.class);

    protected static DatabaseId DBNAME = Identifier.of("myDb");
    protected static EntityType CONTACT = Identifier.of(DBNAME, "contact");
    protected static EntityType CUSTOMER = Identifier.of(DBNAME, "customer");
    protected static String CLIENT_ID = "client1";
    protected static String USERNAME = "jsmith";

    private File stateDir;
    protected ProcessorTopologyTestDriver driver;
    protected Configuration config;
    protected ContentGenerator generator;
    private AtomicLong timestamp;
    private long requestNumber;
    private ProducerRecord<String, Document> lastMessage;

    @Before
    public void setup() throws IOException {
        stateDir = Testing.Files.createTestingDirectory("service-test", true);
        Properties props = new Properties();
        props.setProperty(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        props.setProperty(StreamingConfig.STATE_DIR_CONFIG, stateDir.getAbsolutePath());
        props.setProperty(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomTimestampExtractor.class.getName());
        props.setProperty(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, DocumentSerdes.class.getName());
        props.setProperty(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DocumentSerdes.class.getName());
        
        Properties custom = getCustomConfigurationProperties();
        if ( custom != null ) props.putAll(custom);

        this.config = Configuration.from(props);
        this.generator = CONTENT.createGenerator(6, 10);
        this.timestamp = new AtomicLong(1000L);
        this.requestNumber = 0;
        this.lastMessage = null;
        String[] storeNames = storeNames(config);
        if ( storeNames == null ) storeNames = new String[]{};
        StreamingConfig streamingConfig = new StreamingConfig(props);
        this.driver = new ProcessorTopologyTestDriver(streamingConfig, createTopologyBuilder(config), storeNames);
    }

    @After
    public void cleanup() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
        Testing.Files.delete(stateDir);
    }

    /**
     * Create the {@link TopologyBuilder} for the service.
     * 
     * @param config the streaming configuration
     * @return the topology builder; may not be null
     */
    protected abstract TopologyBuilder createTopologyBuilder(Configuration config);

    /**
     * Return the names of the stores that will be used by the topology.
     * 
     * @param config the streaming configuration
     * @return the store names; may be null or empty
     */
    protected abstract String[] storeNames(Configuration config);

    /**
     * Get any custom configuration properties that should be passed to the topology.
     * @return the custom properties
     */
    protected Properties getCustomConfigurationProperties() {
        return null;
    }
    
    /**
     * Read the next message output by the processor when sent input messages.
     * <p>
     * This method caches the record in the #lastMessage() field for convenience.
     * 
     * @param topic the name of the topic
     * @return the next record, or null if there was no next message on that topic
     */
    protected ProducerRecord<String, Document> nextOutputMessage(String topic) {
        lastMessage = driver.readOutput(topic, Serdes.stringDeserializer(), Serdes.document());
        if (Testing.Debug.isEnabled()) {
            Testing.debug(messageToArray(lastMessage));
        }
        return lastMessage;
    }

    /**
     * Get the last message that was {@link #nextOutputMessage(String) read}.
     * 
     * @return the last message; may be null
     */
    protected ProducerRecord<String, Document> lastMessage() {
        return lastMessage;
    }

    /**
     * Begin an assertion about the last message {@link #nextOutputMessage(String) read}.
     * 
     * @return the message assertion; may be null
     */
    protected MessageAssert assertLastMessage() {
        return assertThat(lastMessage);
    }

    /**
     * Read the next message output by the topology on the given topic, and assert that the key and value match those supplied.
     * <p>
     * This method caches the record in the #lastMessage() field for convenience.
     * 
     * @param topic the name of the output topic
     * @param key the expected key for the output record
     * @param value the expected value for the output record
     */
    protected void assertNextOutputRecord(String topic, String key, Document value) {
        lastMessage = driver.readOutput(topic, Serdes.stringDeserializer(), Serdes.document());
        assertProducerRecord(lastMessage, topic, key, value);
    }

    /**
     * Assert that there are no more messages on the specified output topic.
     * 
     * @param topic the name of the output topic
     */
    protected void assertNoOutputMessages(String topic) {
        assertThat(driver.readOutput(topic)).isNull();
    }

    /**
     * Read the next message output by the processor when sent input messages.
     * <p>
     * This method does <em>NOT</em> cache any output messages in the #lastMessage() field.
     * 
     * @param topic the name of the topic
     * @return the stream of output messages; never null
     */
    protected Stream<ProducerRecord<String, Document>> outputMessages(String topic) {
        List<ProducerRecord<String, Document>> messages = new ArrayList<>();
        do {
            nextOutputMessage(topic);
            if (lastMessage != null) messages.add(lastMessage);
        } while (lastMessage != null);
        if (messages != null && !messages.isEmpty()) {
            return messages.stream();
        }
        return Stream.empty();
    }

    private void assertProducerRecord(ProducerRecord<String, Document> record, String topic, String key, Document value) {
        assertThat(record.topic()).isEqualTo(topic);
        assertThat(record.key()).isEqualTo(key);
        assertThat(record.value()).isEqualTo(value);
        // Kafka Streaming doesn't set the partition, so it's always null
        assertThat(record.partition()).isNull();
    }

    protected Entity createEntity(EntityId id) {
        return generator.generateEntity(id);
    }

    protected Patch<EntityId> createPatch(Entity entity) {
        return Patch.create(entity.id(), entity.asDocument());
    }

    protected Patch.Editor<Patch<EntityId>> edit(EntityId id) {
        return Patch.edit(id);
    }

    protected EntityId generateCustomerId() {
        return Identifier.newEntity(CUSTOMER);
    }

    protected EntityId generateContactId() {
        return Identifier.newEntity(CONTACT);
    }

    protected ContentGenerator generator() {
        return generator;
    }

    protected long timestamp() {
        return timestamp.get();
    }

    protected long advanceTime() {
        return advanceTime(1000);
    }
    
    protected long advanceTime( long incrementInMillis) {
        return timestamp.addAndGet(incrementInMillis);
    }
    
    protected void maybePunctuate() {
// TODO: Add this when it's merged into 'streaming' ...
//        driver.maybePunctuate(timestamp.get());
    }
    
    protected File testFile(String pathInTargetDir) {
        return Testing.Files.createTestingFile(pathInTargetDir);
    }

    protected File resourceFile(String pathInTargetDir) {
        return Testing.Files.createTestingFile(pathInTargetDir);
    }

    protected void printLastMessage() {
        Testing.print(lastMessage());
    }

    protected void printLastMessageDocument() {
        Testing.print(lastMessage().value());
    }

    /**
     * Process the message with the given key and value.
     * 
     * @param topic the name of the topic; may not be null
     * @param key the key; may not be null
     * @param value the value; may not be null
     */
    protected void process(String topic, String key, Document value) {
        driver.process(topic, key, value, Serdes.stringSerializer(), Serdes.document());
    }

    /**
     * Process the request with the given patch request.
     * 
     * @param topic the name of the topic; may not be null
     * @param patch the patch; may not be null
     */
    protected void process(String topic, Patch<EntityId> patch) {
        Document request = patch.asDocument();
        Message.addHeaders(request, CLIENT_ID, ++requestNumber, USERNAME, timestamp());
        Message.setParts(request, 1, 1);
        process(topic, patch.target().toString(), request.clone());
    }

    /**
     * Process all of the records in the given stream into the service.
     * 
     * @param recordsStream the stream containing the records; may not be null
     * @throws IOException if there is an error reading the file
     */
    protected void process(InputStream recordsStream) throws IOException {
        for (ConsumerRecord<String, Document> record : Records.read(recordsStream)) {
            process(record.topic(), record.key(), record.value().clone());
        }
    }

    /**
     * Process all of the records in the given file into the service.
     * 
     * @param recordsFile the file containing the records; may not be null
     * @throws IOException if there is an error reading the file
     */
    protected void process(File recordsFile) throws IOException {
        for (ConsumerRecord<String, Document> record : Records.read(recordsFile)) {
            process(record.topic(), record.key(), record.value());
        }
    }

    protected Array messagesToArray(Iterable<ProducerRecord<String, Document>> records) {
        Array result = Array.create();
        records.forEach(record -> {
            Document message = Document.create();
            message.setString("topic", record.topic());
            if (record.partition() != null) message.setNumber("partition", record.partition().intValue());
            message.setNumber("offset", result.size() + 1);
            message.setString("key", record.key());
            message.setDocument("value", record.value());
            result.add(message);
        });
        return result;
    }

    protected Array messageToArray(ProducerRecord<String, Document> record) {
        Array result = Array.create();
        Document message = Document.create();
        message.setString("topic", record.topic());
        if (record.partition() != null) message.setNumber("partition", record.partition().intValue());
        message.setNumber("offset", result.size() + 1);
        message.setString("key", record.key());
        message.setDocument("value", record.value());
        result.add(message);
        return result;
    }
}
