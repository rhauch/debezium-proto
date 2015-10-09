/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.streams.processor.TopologyBuilder;
import org.debezium.Configuration;
import org.debezium.Testing;
import org.debezium.message.Topic;
import org.debezium.model.EntityCollection.FieldName;
import org.fest.assertions.Delta;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test the SchemaLearningService.
 * 
 * @author Randall Hauch
 */
public class SchemaLearningServiceTest extends TopologyTest {

    private final Delta TOLERANCE = Delta.delta(0.00001f);
    private final long PUNCTUATE_INTERVAL = 30*1000;

    @Override
    protected Properties getCustomConfigurationProperties() {
        Properties props = new Properties();
        props.setProperty("service.id", "learning");
        props.setProperty("service.punctuate.interval.ms", Long.toString(PUNCTUATE_INTERVAL));
        return props;
    }
    
    @Override
    protected TopologyBuilder createTopologyBuilder(Configuration config) {
        return SchemaLearningService.topology(config);
    }
    
    @Override
    protected String[] storeNames(Configuration config) {
        return new String[]{SchemaLearningService.REVISIONS_STORE_NAME, SchemaLearningService.MODELS_STORE_NAME};
    }
    
    @Test
    public void shouldProcessOneEntityUpdateThatHasNotYetBeenSeenAndGenerateSchemaPatch() throws IOException {
        //Testing.Debug.enable();
        process(Testing.Files.readResourceAsStream("entity-updates/single.json"));
        // Read the schema patch ...
        nextOutputMessage(Topic.ENTITY_TYPE_UPDATES);
        printLastMessage();
        assertLastMessage().revision().isEqualTo(1);
    }

    @Test
    public void shouldProcessTwoEntityUpdatesThatHaveNotYetBeenSeenAndGenerateSchemaPatch() throws IOException {
        //Testing.Debug.enable();
        process(Testing.Files.readResourceAsStream("entity-updates/small.json"));
        // Read the schema patch ...
        outputMessages(Topic.ENTITY_TYPE_UPDATES);
        printLastMessage();
        assertLastMessage().revision().isEqualTo(2);
        assertLastMessage().after().documentAt("fields").documentAt("zipCode").floatAt(FieldName.USAGE).isEqualTo(0.5f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("state").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("street").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("phoneNumber").floatAt(FieldName.USAGE).isEqualTo(0.5f, TOLERANCE);
    }

    @Test
    public void shouldProcessTwentyEntityUpdatesThatHaveNotYetBeenSeenAndGenerateSchemaPatch() throws IOException {
        //Testing.Debug.enable();
        process(Testing.Files.readResourceAsStream("entity-updates/medium.json"));
        // Read the schema patch ...
        outputMessages(Topic.ENTITY_TYPE_UPDATES);
        printLastMessage();
        assertLastMessage().revision().isEqualTo(4);
        assertLastMessage().after().documentAt("fields").documentAt("zipCode").floatAt(FieldName.USAGE).isEqualTo(0.5714286f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("state").floatAt(FieldName.USAGE).isEqualTo(0.42857143f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("streetName").floatAt(FieldName.USAGE).isEqualTo(0.14285715f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("phoneNumber").floatAt(FieldName.USAGE).isEqualTo(0.5714286f, TOLERANCE);
    }
    
    @Ignore("Needs to manually punctuate ProcessorTopologyTestDriver")
    @Test
    public void shouldProcessTwoHundredEntityUpdatesThatHaveNotYetBeenSeenAndGenerateSchemaPatch() throws IOException {
        //Testing.Debug.enable();
        process(Testing.Files.readResourceAsStream("entity-updates/large.json"));
        // Read the schema patch ...
        outputMessages(Topic.ENTITY_TYPE_UPDATES);
        printLastMessage();
        assertLastMessage().revision().isEqualTo(3);
        assertLastMessage().after().documentAt("fields").documentAt("zipCode").floatAt(FieldName.USAGE).isEqualTo(0.333333333f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("state").floatAt(FieldName.USAGE).isEqualTo(0.333333333f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("streetName").floatAt(FieldName.USAGE).isEqualTo(0.66666667f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("phoneNumber").floatAt(FieldName.USAGE).isEqualTo(0.66666667f, TOLERANCE);
        assertNoOutputMessages(Topic.ENTITY_TYPE_UPDATES);

        // There are metric changes that haven't been sent to 'entity-type-updates', so simulate passage of time and
        // punctuate to force the output of messages ...
        advanceTime(PUNCTUATE_INTERVAL*2);
        maybePunctuate();
        nextOutputMessage(Topic.ENTITY_TYPE_UPDATES);
        printLastMessage();
        assertLastMessage().revision().isEqualTo(4);
        assertLastMessage().after().documentAt("fields").documentAt("zipCode").floatAt(FieldName.USAGE).isEqualTo(0.515f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("state").floatAt(FieldName.USAGE).isEqualTo(0.555f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("streetName").floatAt(FieldName.USAGE).isEqualTo(0.47f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("phoneNumber").floatAt(FieldName.USAGE).isEqualTo(0.455f, TOLERANCE);
        assertLastMessage().endedTimestamp().isEqualTo(2000);
    }
}
