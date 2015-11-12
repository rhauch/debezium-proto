/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.assertions;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.debezium.message.Document;
import org.debezium.message.Message;
import org.debezium.message.Message.Field;
import org.debezium.message.Message.Status;
import org.fest.assertions.BooleanAssert;
import org.fest.assertions.GenericAssert;
import org.fest.assertions.IntAssert;
import org.fest.assertions.LongAssert;
import org.fest.assertions.StringAssert;

import static org.fest.assertions.Assertions.assertThat;

import static org.fest.assertions.Formatting.format;

/**
 * A specialization of {@link GenericAssert} for Fest utilities.
 * 
 * @author Randall Hauch
 */
public class MessageAssert extends DocumentAssert {

    private final String topic;
    private final Integer partition;

    /**
     * Creates a new {@link MessageAssert}.
     * 
     * @param actual the consumer record whose value is to be verified.
     */
    public MessageAssert(ConsumerRecord<String, Document> actual) {
        this(actual != null ? actual.value() : null,
                actual != null ? actual.topic() : null,
                actual != null ? actual.partition() : new Integer(-1));
    }

    /**
     * Creates a new {@link MessageAssert}.
     * 
     * @param actual the target to verify.
     */
    public MessageAssert(ProducerRecord<String, Document> actual) {
        this(actual != null ? actual.value() : null,
                actual != null ? actual.topic() : null,
                actual != null ? actual.partition() : new Integer(-1));
    }

    protected MessageAssert(Document actual, String topic, Integer partition) {
        super(actual);
        this.topic = topic;
        this.partition = partition;
    }

    public StringAssert topic() {
        return assertThat(topic);
    }

    public StringAssert hasTopic() {
        if ( topic != null ) return assertThat(topic);
        failIfCustomMessageIsSet();
        throw failure(format("expected message to have a topic"));
    }

    public IntAssert partition() {
        return assertThat(partition);
    }

    public IntAssert hasPartition() {
        if ( partition != null ) {
            return assertThat(partition.intValue());
        }
        failIfCustomMessageIsSet();
        throw failure(format("expected message to have a partition but it was null"));
    }

    public MessageAssert hasTopic(String expected) {
        isNotNull();
        validateValueNotNull(expected);
        if (topic.equals(expected)) return this;
        failIfCustomMessageIsSet();
        throw failure(format("expected topic was <%s> but found <%s>", expected, this.topic));
    }

    @Override
    public MessageAssert hasField(CharSequence fieldName) {
        super.hasField(fieldName);
        return this;
    }

    @Override
    public MessageAssert hasField(CharSequence fieldName, Object value) {
        super.hasField(fieldName, value);
        return this;
    }

    @Override
    public MessageAssert hasNoFieldAt(CharSequence fieldName) {
        super.hasNoFieldAt(fieldName);
        return this;
    }

    public MessageAssert hasRequiredHeaderFields() {
        for (String field : Message.requiredHeaderFields()) {
            hasField(field);
        }
        return this;
    }

    public MessageAssert hasStatus(Status expected) {
        return hasField(Field.STATUS, expected.code());
    }

    public IntAssert status() {
        return integerAt(Field.STATUS);
    }

    public MessageAssert hasNoFailureMessage() {
        return hasNoFieldAt(Field.ERROR);
    }

    public MessageAssert hasFailureMessage() {
        hasStringAt(Field.ERROR);
        return this;
    }

    public MessageAssert hasFailureMessages() {
        hasArrayAt(Field.ERROR);
        return this;
    }

    public StringAssert failureMessage() {
        return stringAt(Field.ERROR);
    }

    public ArrayAssert failureMessages() {
        return arrayAt(Field.ERROR);
    }

    public StringAssert clientId() {
        return stringAt(Field.CLIENT_ID);
    }

    public StringAssert username() {
        return stringAt(Field.USER);
    }

    public LongAssert requestNumber() {
        return longAt(Field.REQUEST);
    }

    public IntAssert partCount() {
        return integerAt(Field.PART);
    }

    public IntAssert partsCount() {
        return integerAt(Field.PARTS);
    }

    public IntAssert revision() {
        return integerAt(Field.REVISION);
    }

    public ArrayAssert responses() {
        return arrayAt(Field.RESPONSES);
    }

    public LongAssert begunTimestamp() {
        return longAt(Field.BEGUN);
    }

    public LongAssert endedTimestamp() {
        return longAt(Field.ENDED);
    }

    public StringAssert databaseId() {
        return stringAt(Field.DATABASE_ID);
    }

    public StringAssert collection() {
        return stringAt(Field.COLLECTION);
    }

    public StringAssert zoneId() {
        return stringAt(Field.ZONE_ID);
    }

    public StringAssert entityId() {
        return stringAt(Field.ENTITY);
    }

    public BooleanAssert learning() {
        return booleanAt(Field.LEARNING);
    }

    public BooleanAssert includeBefore() {
        return booleanAt(Field.INCLUDE_BEFORE);
    }

    public BooleanAssert includeAfter() {
        return booleanAt(Field.INCLUDE_AFTER);
    }

    public DocumentAssert before() {
        return documentAt(Field.BEFORE);
    }

    public DocumentAssert after() {
        return documentAt(Field.AFTER);
    }
}
