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
import org.junit.Test;

/**
 * Test the SchemaLearningService.
 * 
 * @author Randall Hauch
 */
public class SchemaServiceTest extends TopologyTest {

    private final Delta TOLERANCE = Delta.delta(0.00001f);
    private final long PUNCTUATE_INTERVAL = 30 * 1000;

    @Override
    protected Properties getCustomConfigurationProperties() {
        Properties props = new Properties();
        props.setProperty("service.id", "learning");
        props.setProperty("service.punctuate.interval.ms", Long.toString(PUNCTUATE_INTERVAL));
        return props;
    }

    @Override
    protected TopologyBuilder createTopologyBuilder(Configuration config) {
        return SchemaService.topology(config);
    }

    @Override
    protected String[] storeNames(Configuration config) {
        return new String[] { SchemaService.REVISIONS_STORE_NAME, SchemaService.MODELS_STORE_NAME,
                SchemaService.MODEL_OVERRIDES_STORE_NAME };
    }

    @Test
    public void shouldProcessOneEntityUpdateThatHasNotYetBeenSeenAndGenerateSchemaPatch() throws IOException {
        // Testing.Print.enable();
        process(Testing.Files.readResourceAsStream("entity-type-updates/service1-single.json"));
        // Read the schema patch ...
        nextOutputMessage(Topic.SCHEMA_UPDATES);
        printLastMessage();
        assertLastMessage().revision().isEqualTo(1);
        assertLastMessage().after().documentAt("fields").documentAt("firstName").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("firstName").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("lastName").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("lastName").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("streetNumber").stringAt(FieldName.TYPE).isEqualTo("INTEGER");
        assertLastMessage().after().documentAt("fields").documentAt("streetNumber").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("street").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("street").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("city").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("city").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("state").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("state").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("age").stringAt(FieldName.TYPE).isEqualTo("INTEGER");
        assertLastMessage().after().documentAt("fields").documentAt("age").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("phoneNumber").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("phoneNumber").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").size().isEqualTo(8);
    }

    @Test
    public void shouldProcessTwoEntityUpdatesThatHaveNotYetBeenSeenAndGenerateSchemaPatch() throws IOException {
        // Testing.Debug.enable();
        process(Testing.Files.readResourceAsStream("entity-type-updates/service1-small.json"));
        // Read the schema patch ...
        outputMessages(Topic.SCHEMA_UPDATES);
        printLastMessage();
        assertLastMessage().revision().isEqualTo(2);
        assertLastMessage().after().documentAt("fields").documentAt("firstName").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("firstName").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("lastName").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("lastName").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("streetNumber").stringAt(FieldName.TYPE).isEqualTo("INTEGER");
        assertLastMessage().after().documentAt("fields").documentAt("streetNumber").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("street").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("street").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("city").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("city").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("state").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("state").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("age").stringAt(FieldName.TYPE).isEqualTo("INTEGER");
        assertLastMessage().after().documentAt("fields").documentAt("age").floatAt(FieldName.USAGE).isEqualTo(0.5f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("phoneNumber").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("phoneNumber").floatAt(FieldName.USAGE).isEqualTo(0.5f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("middleName").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("middleName").floatAt(FieldName.USAGE).isEqualTo(0.5f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("zipCode").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("zipCode").floatAt(FieldName.USAGE).isEqualTo(0.5f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").size().isEqualTo(10);
        assertLastMessage().after().documentAt("$metrics").integerAt("totalAdds").isEqualTo(2);
        assertLastMessage().after().documentAt("$metrics").integerAt("recentAdds").isEqualTo(1);
    }

    @Test
    public void shouldProcessTwentyEntityUpdatesThatHaveNotYetBeenSeenAndGenerateSchemaPatch() throws IOException {
        // Testing.Print.enable();
        process(Testing.Files.readResourceAsStream("entity-type-updates/service1-medium.json"));
        // Read the schema patch ...
        nextOutputMessage(Topic.SCHEMA_UPDATES);
        printLastMessage();
        assertLastMessage().revision().isEqualTo(1);
        assertLastMessage().after().documentAt("$metrics").integerAt("totalAdds").isEqualTo(1);
        assertLastMessage().after().documentAt("$metrics").integerAt("recentAdds").isEqualTo(1);
        assertLastMessage().after().documentAt("fields").documentAt("zipCode").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("zipCode").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("phoneNumber").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("phoneNumber").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("streetNumber").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("streetNumber").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("birthMonth").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("birthMonth").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("city").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("city").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("birthYear").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("birthYear").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("firstName").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("firstName").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").size().isEqualTo(7);

        nextOutputMessage(Topic.SCHEMA_UPDATES);
        printLastMessage();
        assertLastMessage().revision().isEqualTo(2);
        assertLastMessage().after().documentAt("$metrics").integerAt("totalAdds").isEqualTo(2);
        assertLastMessage().after().documentAt("$metrics").integerAt("recentAdds").isEqualTo(1);
        assertLastMessage().after().documentAt("fields").documentAt("zipCode").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("zipCode").floatAt(FieldName.USAGE).isEqualTo(0.5f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("phoneNumber").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("phoneNumber").floatAt(FieldName.USAGE).isEqualTo(0.5f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("streetNumber").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("streetNumber").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("birthMonth").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("birthMonth").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("city").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("city").floatAt(FieldName.USAGE).isEqualTo(0.5f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("birthYear").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("birthYear").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("firstName").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("firstName").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("lastName").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("lastName").floatAt(FieldName.USAGE).isEqualTo(0.5f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").size().isEqualTo(8);

        nextOutputMessage(Topic.SCHEMA_UPDATES);
        printLastMessage();
        assertLastMessage().revision().isEqualTo(3);
        assertLastMessage().after().documentAt("$metrics").integerAt("totalAdds").isEqualTo(3);
        assertLastMessage().after().documentAt("$metrics").integerAt("recentAdds").isEqualTo(1);
        assertLastMessage().after().documentAt("fields").documentAt("zipCode").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("zipCode").floatAt(FieldName.USAGE).isEqualTo(0.33333334f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("phoneNumber").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("phoneNumber").floatAt(FieldName.USAGE).isEqualTo(0.6666667f,
                                                                                                                      TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("streetNumber").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("streetNumber").floatAt(FieldName.USAGE).isEqualTo(0.6666667f,
                                                                                                                       TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("birthMonth").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("birthMonth").floatAt(FieldName.USAGE).isEqualTo(0.6666667f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("city").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("city").floatAt(FieldName.USAGE).isEqualTo(0.6666667f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("birthYear").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("birthYear").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("firstName").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("firstName").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("lastName").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("lastName").floatAt(FieldName.USAGE).isEqualTo(0.6666667f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("state").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("state").floatAt(FieldName.USAGE).isEqualTo(0.33333334f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").size().isEqualTo(9);

        nextOutputMessage(Topic.SCHEMA_UPDATES);
        printLastMessage();
        assertLastMessage().revision().isEqualTo(4);
        assertLastMessage().after().documentAt("$metrics").integerAt("totalAdds").isEqualTo(7);
        assertLastMessage().after().documentAt("$metrics").integerAt("recentAdds").isEqualTo(4);
        assertLastMessage().after().documentAt("fields").documentAt("zipCode").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("zipCode").floatAt(FieldName.USAGE).isEqualTo(0.5714286f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("phoneNumber").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("phoneNumber").floatAt(FieldName.USAGE).isEqualTo(0.5714286f,
                                                                                                                      TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("streetNumber").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("streetNumber").floatAt(FieldName.USAGE).isEqualTo(0.42857143f,
                                                                                                                       TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("birthMonth").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("birthMonth").floatAt(FieldName.USAGE).isEqualTo(0.42857143f,
                                                                                                                     TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("city").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("city").floatAt(FieldName.USAGE).isEqualTo(0.5714286f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("birthYear").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("birthYear").floatAt(FieldName.USAGE).isEqualTo(0.42857143f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("firstName").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("firstName").floatAt(FieldName.USAGE).isEqualTo(1.0f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("lastName").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("lastName").floatAt(FieldName.USAGE).isEqualTo(0.5714286f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("state").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("state").floatAt(FieldName.USAGE).isEqualTo(0.42857143f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("streetName").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("streetName").floatAt(FieldName.USAGE).isEqualTo(0.14285715f,
                                                                                                                     TOLERANCE);
        assertLastMessage().after().documentAt("fields").size().isEqualTo(10);
    }

    @Test
    public void shouldProcessTwoHundredEntityUpdatesThatHaveNotYetBeenSeenAndGenerateSchemaPatch() throws IOException {
        // Testing.Print.enable();
        process(Testing.Files.readResourceAsStream("entity-type-updates/service1-large.json"));
        // Read the schema patch ...
        nextOutputMessage(Topic.SCHEMA_UPDATES);
        printLastMessage();
        assertLastMessage().revision().isEqualTo(3);
        assertLastMessage().after().documentAt("$metrics").integerAt("totalAdds").isEqualTo(3);
        assertLastMessage().after().documentAt("$metrics").integerAt("recentAdds").isEqualTo(1);
        assertLastMessage().after().documentAt("fields").documentAt("zipCode").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("zipCode").floatAt(FieldName.USAGE).isEqualTo(0.33333334f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("phoneNumber").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("phoneNumber").floatAt(FieldName.USAGE).isEqualTo(0.6666667f,
                                                                                                                      TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("streetNumber").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("streetNumber").floatAt(FieldName.USAGE).isEqualTo(0.33333334f,
                                                                                                                       TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("birthMonth").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("birthMonth").floatAt(FieldName.USAGE).isEqualTo(0.6666667f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("city").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("city").floatAt(FieldName.USAGE).isEqualTo(0.6666667f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("birthYear").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("birthYear").floatAt(FieldName.USAGE).isEqualTo(0.33333334f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("firstName").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("firstName").floatAt(FieldName.USAGE).isEqualTo(0.6666667f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("lastName").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("lastName").floatAt(FieldName.USAGE).isEqualTo(0.6666667f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("state").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("state").floatAt(FieldName.USAGE).isEqualTo(0.33333334f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("streetName").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("streetName").floatAt(FieldName.USAGE).isEqualTo(0.6666667f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").size().isEqualTo(10);
        assertLastMessage().endedTimestamp().isEqualTo(1000);
        assertNoOutputMessages(Topic.SCHEMA_UPDATES);

        // Simulate that time passes ...
        advanceTime(PUNCTUATE_INTERVAL * 2);
        // Now, punctuate the service so that it updates statistics ...
        maybePunctuate();
        outputMessages(Topic.SCHEMA_UPDATES);
        printLastMessage();
        assertLastMessage().revision().isEqualTo(4);
        assertLastMessage().after().hasNoFieldAt("$metrics");
        assertLastMessage().after().documentAt("fields").documentAt("zipCode").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("zipCode").floatAt(FieldName.USAGE).isEqualTo(0.515f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("phoneNumber").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("phoneNumber").floatAt(FieldName.USAGE).isEqualTo(0.455f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("streetNumber").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("streetNumber").floatAt(FieldName.USAGE).isEqualTo(0.46f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("birthMonth").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("birthMonth").floatAt(FieldName.USAGE).isEqualTo(0.59f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("city").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("city").floatAt(FieldName.USAGE).isEqualTo(0.425f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("birthYear").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("birthYear").floatAt(FieldName.USAGE).isEqualTo(0.525f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("firstName").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("firstName").floatAt(FieldName.USAGE).isEqualTo(0.435f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("lastName").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("lastName").floatAt(FieldName.USAGE).isEqualTo(0.53f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("state").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("state").floatAt(FieldName.USAGE).isEqualTo(0.555f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").documentAt("streetName").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().after().documentAt("fields").documentAt("streetName").floatAt(FieldName.USAGE).isEqualTo(0.47f, TOLERANCE);
        assertLastMessage().after().documentAt("fields").size().isEqualTo(10);
        assertLastMessage().after().longAt("ended").isEqualTo(2000);

        assertLastMessage().before().documentAt("fields").documentAt("zipCode").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().before().documentAt("fields").documentAt("zipCode").floatAt(FieldName.USAGE).isEqualTo(0.33333334f, TOLERANCE);
        assertLastMessage().before().documentAt("fields").documentAt("phoneNumber").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().before().documentAt("fields").documentAt("phoneNumber").floatAt(FieldName.USAGE).isEqualTo(0.6666667f,
                                                                                                                       TOLERANCE);
        assertLastMessage().before().documentAt("fields").documentAt("streetNumber").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().before().documentAt("fields").documentAt("streetNumber").floatAt(FieldName.USAGE).isEqualTo(0.33333334f,
                                                                                                                        TOLERANCE);
        assertLastMessage().before().documentAt("fields").documentAt("birthMonth").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().before().documentAt("fields").documentAt("birthMonth").floatAt(FieldName.USAGE).isEqualTo(0.6666667f,
                                                                                                                      TOLERANCE);
        assertLastMessage().before().documentAt("fields").documentAt("city").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().before().documentAt("fields").documentAt("city").floatAt(FieldName.USAGE).isEqualTo(0.6666667f, TOLERANCE);
        assertLastMessage().before().documentAt("fields").documentAt("birthYear").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().before().documentAt("fields").documentAt("birthYear").floatAt(FieldName.USAGE).isEqualTo(0.33333334f,
                                                                                                                     TOLERANCE);
        assertLastMessage().before().documentAt("fields").documentAt("firstName").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().before().documentAt("fields").documentAt("firstName").floatAt(FieldName.USAGE).isEqualTo(0.6666667f, TOLERANCE);
        assertLastMessage().before().documentAt("fields").documentAt("lastName").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().before().documentAt("fields").documentAt("lastName").floatAt(FieldName.USAGE).isEqualTo(0.6666667f, TOLERANCE);
        assertLastMessage().before().documentAt("fields").documentAt("state").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().before().documentAt("fields").documentAt("state").floatAt(FieldName.USAGE).isEqualTo(0.33333334f, TOLERANCE);
        assertLastMessage().before().documentAt("fields").documentAt("streetName").stringAt(FieldName.TYPE).isEqualTo("STRING");
        assertLastMessage().before().documentAt("fields").documentAt("streetName").floatAt(FieldName.USAGE).isEqualTo(0.6666667f,
                                                                                                                      TOLERANCE);
        assertLastMessage().before().documentAt("fields").size().isEqualTo(10);
        assertLastMessage().before().longAt("ended").isEqualTo(1000);

        assertLastMessage().endedTimestamp().isEqualTo(2000);
    }
}
