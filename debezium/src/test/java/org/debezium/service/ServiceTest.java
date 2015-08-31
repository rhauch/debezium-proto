/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.processor.KafkaProcessor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.debezium.Testing;
import org.debezium.assertions.MessageAssert;
import org.debezium.message.Array;
import org.debezium.message.Document;
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

import static org.debezium.assertions.DebeziumAssertions.assertThat;

import static org.fest.assertions.Assertions.assertThat;

/**
 * A base class for test cases that verify the functionality of a service's {@link KafkaProcessor} implementation.
 * 
 * @author Randall Hauch
 */
public abstract class ServiceTest implements Testing {

    protected static final RandomContent CONTENT = RandomContent.load("load-data.txt", RandomContentTest.class);

    protected static DatabaseId DBNAME = Identifier.of("myDb");
    protected static EntityType CONTACT = Identifier.of(DBNAME, "contact");
    protected static EntityType CUSTOMER = Identifier.of(DBNAME, "customer");
    protected static String CLIENT_ID = "client1";
    protected static String USERNAME = "jsmith";

    private final ConcurrentMap<String, AtomicLong> offsetsByTopic = new ConcurrentHashMap<>();
    private final AtomicLong timestamp = new AtomicLong(1000L);
    private MockProcessorContext<String, Document> mockContext;
    private KafkaProcessor<String, Document, String, Document> processor;
    private ConcurrentMap<String, LinkedList<ProducerRecord<String, Document>>> outputMessages = new ConcurrentHashMap<>();
    private ProducerRecord<String, Document> lastMessage;
    private long requestNumber;
    private ContentGenerator generator;

    @Before
    public void beforeEach() throws IOException {
        resetBeforeEachTest();
        mockContext = MockProcessorContext.forDebezium(record -> {
            outputMessages.computeIfAbsent(record.topic(), key -> new LinkedList<ProducerRecord<String, Document>>()).add(record);
        });
        processor = createProcessor();
        processor.init(mockContext);
        requestNumber = 0;
        generator = CONTENT.createGenerator(6, 10);
    }

    @After
    public void afterEach() {
        processor.close();
    }

    /**
     * Create an instance of the service's {@link KafkaProcessor}.
     * 
     * @return the processor; may not be null
     */
    protected abstract KafkaProcessor<String, Document, String, Document> createProcessor();

    /**
     * Read the next message output by the processor when sent input messages.
     * <p>
     * This method caches the record in the #lastMessage() field for convenience.
     * 
     * @param topic the name of the topic
     * @return the next record, or null if there was no next message on that topic
     */
    protected ProducerRecord<String, Document> nextOutputMessage(String topic) {
        LinkedList<ProducerRecord<String, Document>> messages = outputMessages.get(topic);
        if (messages != null && !messages.isEmpty()) {
            lastMessage = messages.removeFirst();
        } else {
            lastMessage = null;
        }
        if (Testing.Debug.isEnabled()) {
            Testing.debug(messageToArray(lastMessage));
        }
        return lastMessage;
    }

    /**
     * Read all available messages output by the processor when sent input messages.
     * <p>
     * This method caches the record in the #lastMessage() field for convenience.
     * 
     * @param topic the name of the topic
     * @return the last record read, or null if there was no next message on that topic
     */
    protected ProducerRecord<String, Document> readAllOutputMessages(String topic) {
        LinkedList<ProducerRecord<String, Document>> messages = outputMessages.get(topic);
        if (Testing.Debug.isEnabled()) {
            Testing.debug(messagesToArray(messages));
        }
        if (messages != null && !messages.isEmpty()) {
            lastMessage = messages.getLast();
            messages.clear();
        } else {
            lastMessage = null;
        }
        return lastMessage;
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

    /**
     * Read the next message output by the processor when sent input messages.
     * <p>
     * This method does <em>NOT</em> cache any output messages in the #lastMessage() field.
     * 
     * @param topic the name of the topic
     * @return the stream of output messages; never null
     */
    protected Stream<ProducerRecord<String, Document>> outputMessages(String topic) {
        LinkedList<ProducerRecord<String, Document>> messages = outputMessages.get(topic);
        if (messages != null && !messages.isEmpty()) {
            return messages.stream();
        }
        return Stream.empty();
    }

    protected void assertNoOutputMessages(String topic) {
        LinkedList<ProducerRecord<String, Document>> messages = outputMessages.get(topic);
        if ( messages != null ) assertThat(messages.isEmpty()).isTrue();
    }

    protected ProducerRecord<String, Document> lastMessage() {
        return lastMessage;
    }

    protected MessageAssert assertLastMessage() {
        return assertThat(lastMessage);
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

    private long nextOffset(String topic) {
        return offsetsByTopic.computeIfAbsent(topic, key -> new AtomicLong()).incrementAndGet();
    }

    protected long timestamp() {
        return timestamp.get();
    }

    protected long advanceTime() {
        return timestamp.addAndGet(1000);
    }

    protected void send(String topic, String key, Document value) {
        // Update the context first ...
        mockContext.setTopic(topic);
        mockContext.setOffset(nextOffset(topic));
        mockContext.setPartition(1);
        mockContext.setTime(timestamp());
        // Then invoke the processor ...
        processor.process(key, value.clone());
        // Automatically invoke the commit ...
        mockContext.flush();
        mockContext.commit();
    }

    protected void punctuate() {
        mockContext.setTime(timestamp());
        // Then invoke the processor ...
        processor.punctuate(timestamp());
        // Automatically invoke the commit ...
        mockContext.flush();
        mockContext.commit();
    }

    /**
     * Create a request for the given patch.
     * 
     * @param topic the name of the topic; may not be null
     * @param patch the patch; may not be null
     */
    protected void send(String topic, Patch<EntityId> patch) {
        Document request = patch.asDocument();
        Message.addHeaders(request, CLIENT_ID, ++requestNumber, USERNAME, timestamp());
        Message.setParts(request, 1, 1);
        send(topic, patch.target().toString(), request.clone());
    }

    /**
     * Send all of the records in the given stream into the service.
     * 
     * @param recordsStream the stream containing the records; may not be null
     * @throws IOException if there is an error reading the file
     */
    protected void send(InputStream recordsStream) throws IOException {
        for (ConsumerRecord<String, Document> record : Records.read(recordsStream)) {
            send(record.topic(), record.key(), record.value().clone());
        }
    }

    /**
     * Send all of the records in the given file into the service.
     * 
     * @param recordsFile the file containing the records; may not be null
     * @throws IOException if there is an error reading the file
     */
    protected void send(File recordsFile) throws IOException {
        for (ConsumerRecord<String, Document> record : Records.read(recordsFile)) {
            send(record.topic(), record.key(), record.value());
        }
    }

    /**
     * Read all of the output messages on the given topic and write them to the specified file.
     * 
     * @param topicName the name of the topic to read
     * @param recordsFile the file into which the records should be written; may not be null
     * @throws IOException if there is an error writing the file
     */
    protected void writeOutputMessages(String topicName, File recordsFile) throws IOException {
        Records.write(recordsFile, outputMessages(topicName));
    }

    protected long lastRequestNumber() {
        return requestNumber;
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

    protected Entity createEntity(EntityId id) {
        return generator.generateEntity(id);
    }

    protected Patch<EntityId> createPatch(Entity entity) {
        return Patch.create(entity.id(), entity.document());
    }

    protected Patch.Editor<Patch<EntityId>> edit(EntityId id) {
        return Patch.edit(id);
    }
}
