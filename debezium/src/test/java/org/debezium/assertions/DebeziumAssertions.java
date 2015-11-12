/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.assertions;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.debezium.message.Array;
import org.debezium.message.Batch;
import org.debezium.message.Document;
import org.debezium.message.Patch;
import org.debezium.message.Value;
import org.debezium.model.Identifier;

/**
 * Custom Fest assertion methods.
 * @author Randall Hauch
 */
public class DebeziumAssertions {

    /**
     * Creates a new instance of <code>{@link DocumentAssert}</code>.
     * @param actual the value to be the target of the assertions methods.
     * @return the created assertion object.
     */
    public static DocumentAssert assertThat(Document actual) {
      return new DocumentAssert(actual);
    }

    /**
     * Creates a new instance of <code>{@link ArrayAssert}</code>.
     * @param actual the value to be the target of the assertions methods.
     * @return the created assertion object.
     */
    public static ArrayAssert assertThat(Array actual) {
      return new ArrayAssert(actual);
    }

    /**
     * Creates a new instance of <code>{@link PatchAssert}</code>.
     * @param actual the value to be the target of the assertions methods.
     * @return the created assertion object.
     */
    public static PatchAssert assertThat(Patch<? extends Identifier> actual) {
      return new PatchAssert(actual);
    }

    /**
     * Creates a new instance of <code>{@link BatchAssert}</code>.
     * @param actual the value to be the target of the assertions methods.
     * @return the created assertion object.
     */
    public static BatchAssert assertThat(Batch<? extends Identifier> actual) {
      return new BatchAssert(actual);
    }

    /**
     * Creates a new instance of <code>{@link MessageAssert}</code>.
     * @param actual the value to be the target of the assertions methods.
     * @return the created assertion object.
     */
    public static MessageAssert assertThat(ProducerRecord<String,Document> actual) {
      return new MessageAssert(actual);
    }

    /**
     * Creates a new instance of <code>{@link MessageAssert}</code>.
     * @param actual the value to be the target of the assertions methods.
     * @return the created assertion object.
     */
    public static MessageAssert assertThat(ConsumerRecord<String,Document> actual) {
      return new MessageAssert(actual);
    }

    /**
     * Creates a new instance of <code>{@link ValueAssert}</code>.
     * @param actual the value to be the target of the assertions methods.
     * @return the created assertion object.
     */
    public static ValueAssert assertThat(Value actual) {
      return new ValueAssert(actual);
    }

}
