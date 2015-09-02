/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.processor.KafkaProcessor;
import org.apache.kafka.clients.processor.ProcessorProperties;
import org.debezium.message.Document;
import org.debezium.message.Message.Field;
import org.debezium.message.Message.Status;
import org.debezium.message.Patch;
import org.debezium.message.Records;
import org.debezium.message.Topic;
import org.debezium.model.Entity;
import org.debezium.model.EntityId;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the EntityStorageService.
 * 
 * @author Randall Hauch
 */
public class EntityStorageServiceTest extends ServiceTest {

    @Override
    protected KafkaProcessor<String, Document, String, Document> createProcessor() {
        return new EntityStorageService(new ProcessorProperties(new Properties()));
    }

    @Override
    @Before
    public void beforeEach() throws IOException {
        super.beforeEach();
    }

    @Test
    public void shouldSendFailureResponseWhenReadingNonExistantEntity() {
        EntityId contactId = generateContactId();
        send(Topic.ENTITY_PATCHES, Patch.read(contactId));
        // Read the partial response ...
        nextOutputMessage(Topic.PARTIAL_RESPONSES);
        assertLastMessageIsPartialResponse();
        assertLastMessage().hasStatus(Status.DOES_NOT_EXIST);
        assertLastMessage().hasFailureMessage();
        assertLastMessage().partCount().isEqualTo(1);
        assertLastMessage().partsCount().isEqualTo(1);
        // Ensure no update responses ...
        assertNoOutputMessages(Topic.ENTITY_UPDATES);
    }

    @Test
    public void shouldCreateNewEntityWhenPatchingNonExistantEntity() {
        // Testing.Print.enable();
        Entity contact1 = createEntity(generateContactId());
        send(Topic.ENTITY_PATCHES, createPatch(contact1));
        // Read the partial response ...
        nextOutputMessage(Topic.PARTIAL_RESPONSES);
        assertLastMessageIsPartialResponse();
        assertLastMessage().hasStatus(Status.SUCCESS);
        assertLastMessage().hasNoFailureMessage();
        assertLastMessage().partCount().isEqualTo(1);
        assertLastMessage().partsCount().isEqualTo(1);
        assertLastMessage().before().isNull();
        assertLastMessage().after().hasAll(contact1.asDocument());
        assertLastMessage().revision().isEqualTo(1);
        printLastMessage();
        // Read the update response ...
        nextOutputMessage(Topic.ENTITY_UPDATES);
        assertLastMessageIsEntityUpdateResponse();
        assertLastMessage().hasStatus(Status.SUCCESS);
        assertLastMessage().hasNoFailureMessage();
        assertLastMessage().partCount().isEqualTo(1);
        assertLastMessage().partsCount().isEqualTo(1);
        assertLastMessage().before().isNull();
        assertLastMessage().after().hasAll(contact1.asDocument());
        assertLastMessage().after().integerAt(Field.REVISION).isEqualTo(1);
        assertLastMessage().revision().isEqualTo(1);
        printLastMessage();
    }

    @Test
    public void shouldRecordArrayOfTwoEntityUpdateMessages() throws IOException {
        generateInput(2);
        Records.write(testFile("entity-updates-small.json"), outputMessages(Topic.ENTITY_UPDATES));
    }

    @Test
    public void shouldRecordArrayOfTwentyEntityUpdateMessages() throws IOException {
        generateInput(20);
        Records.write(testFile("entity-updates-medium.json"), outputMessages(Topic.ENTITY_UPDATES));
    }

    @Test
    public void shouldRecordArrayOfTwoHundredUpdateMessages() throws IOException {
        generateInput(200);
        Records.write(testFile("entity-updates-large.json"), outputMessages(Topic.ENTITY_UPDATES));
    }

    protected void generateInput(int count) {
        for (int i = 0; i != count; ++i) {
            Entity contact1 = createEntity(generateContactId());
            send(Topic.ENTITY_PATCHES, createPatch(contact1));
        }
    }

    protected void assertLastMessageIsPartialResponse() {
        assertLastMessage().topic().isEqualTo(Topic.PARTIAL_RESPONSES);
        assertLastMessage().clientId().isEqualTo(CLIENT_ID);
        assertLastMessage().username().isEqualTo(USERNAME);
        assertLastMessage().hasRequiredHeaderFields();
        assertLastMessage().partCount().isGreaterThan(0);
        assertLastMessage().partsCount().isGreaterThan(0);
    }

    protected void assertLastMessageIsEntityUpdateResponse() {
        assertLastMessage().topic().isEqualTo(Topic.ENTITY_UPDATES);
        assertLastMessage().clientId().isEqualTo(CLIENT_ID);
        assertLastMessage().username().isEqualTo(USERNAME);
        assertLastMessage().hasRequiredHeaderFields();
        assertLastMessage().partCount().isGreaterThan(0);
        assertLastMessage().partsCount().isGreaterThan(0);
    }
}
