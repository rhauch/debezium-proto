/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.samza.system.OutgoingMessageEnvelope;
import org.debezium.Testing;
import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.Identifier;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.DocumentReader;
import org.debezium.core.message.Batch;
import org.debezium.core.message.Message;
import org.debezium.core.message.Topic;
import org.debezium.samza.AbstractServiceTest;
import org.debezium.service.MetricService.MetricFields;
import org.fest.assertions.Condition;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Randall Hauch
 */
public class MetricServiceTest extends AbstractServiceTest {

    private static final DatabaseId DB_ID = Identifier.of("db");
    private static final String CLIENT_ID = "test-client-1";
    private static final String FOLDER_NAME = Topic.COMPLETE_RESPONSES;

    private MetricService service;

    @Before
    public void beforeEach() {
        service = new MetricService();
        service.init(testConfig(), testContext());
    }

    protected Document batchToDocumentWithDbId(Batch<?> batch) {
        Document message = batch.asDocument();
        Message.addHeaders(message, CLIENT_ID);
        Message.addId(message, DB_ID);
        return message;
    }

    @Test
    public void shouldHandleEmptyBatch() {
        Batch<Identifier> batch = Batch.create().build();
        OutputMessages output = process(service, random(), batchToDocumentWithDbId(batch));
        assertThat(output.isEmpty()).isTrue();
        assertNoMoreMessages(output);
        // Generate the statistics ...
        OutputMessages windowOutput = window(service);
        assertNextMetricMessage(windowOutput).hasStream(Topic.METRICS).hasEntityCounts(0, 0, 0, 0).hasValidStatistics();
        assertNoMoreMessages(windowOutput);
    }

    @Test
    public void shouldHandleBatchWithOnePatch() throws IOException {
        //Testing.Print.enable();
        String json = Testing.Files.readResourceAsString(FOLDER_NAME + "/create-contact-1.json");
        Document completeResponse = DocumentReader.defaultReader().read(json);
        OutputMessages output = process(service, random(), completeResponse);
        // The metric service only produces output messages upon 'window' ...
        assertNoMoreMessages(output);
        // Generate the statistics ...
        OutputMessages windowOutput = window(service);
        assertNextMetricMessage(windowOutput).hasStream(Topic.METRICS).hasEntityCounts(1, 1, 0, 0).hasValidStatistics();
        assertNoMoreMessages(windowOutput);
    }

    @Test
    public void shouldHandleBatchWithTwoPatches() throws IOException {
        //Testing.Print.enable();
        String json = Testing.Files.readResourceAsString(FOLDER_NAME + "/create-contact-2.json");
        Document completeResponse = DocumentReader.defaultReader().read(json);
        OutputMessages output = process(service, random(), completeResponse);
        // The metric service only produces output messages upon 'window' ...
        assertNoMoreMessages(output);
        // Generate the statistics ...
        OutputMessages windowOutput = window(service);
        assertNextMetricMessage(windowOutput).hasStream(Topic.METRICS).hasEntityCounts(2, 2, 0, 0).hasValidStatistics();
        assertNoMoreMessages(windowOutput);
    }

    @Test
    public void shouldHandleManyMessages() throws IOException {
        String json = Testing.Files.readResourceAsString(FOLDER_NAME + "/create-contact-2.json");
        Document completeResponse = DocumentReader.defaultReader().read(json);
        int batches = 1000;
        for (int i = 0; i != batches; ++i) {
            // We just repeat the same batch outputs; the metric service doesn't care ...
            OutputMessages output = process(service, random(), completeResponse);
            // The metric service only produces output messages upon 'window' ...
            assertNoMoreMessages(output);
        }
        // Generate the statistics ...
        OutputMessages windowOutput = window(service);
        assertNextMetricMessage(windowOutput).hasStream(Topic.METRICS).hasEntityCounts(2 * batches, 2 * batches, 0, 0).hasValidStatistics();
        assertNoMoreMessages(windowOutput);
    }

    @Ignore
    @Test
    public void shouldHandleMessagesFor6Minutes() throws IOException {
        //Testing.Print.enable();
        String json = Testing.Files.readResourceAsString(FOLDER_NAME + "/create-contact-2.json");
        Document completeResponse = DocumentReader.defaultReader().read(json);
        Testing.print("Starting ...");
        for (int t = 0; t != 6; ++t) {
            // Work for 1 minute ...
            long stopTime = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1);
            while (System.currentTimeMillis() <= stopTime) {
                // Loop so that we don't have to check the clock every nanosecond ...
                for (int i = 0; i != 1000; ++i) {
                    // We just repeat the same batch outputs; the metric service doesn't care ...
                    OutputMessages output = process(service, random(), completeResponse);
                    // The metric service only produces output messages upon 'window' ...
                    assertNoMoreMessages(output);
                }
            }
            Testing.print("Completed " + (t + 1) + " minute(s)");
            // Generate the statistics ...
            OutputMessages windowOutput = window(service);
            assertNextMetricMessage(windowOutput).hasStream(Topic.METRICS)
                                                 .hasValidStatistics();
            assertNoMoreMessages(windowOutput);
        }
    }

    protected StatisticValidator assertNextMetricMessage(OutputMessages messages) {
        OutgoingMessageEnvelope env = messages.removeFirst();
        Document message = (Document) env.getMessage();
        return new StatisticValidator() {
            @Override
            public StatisticValidator hasPartitionKey(Object partitionKey) {
                assertThat(env.getPartitionKey()).isEqualTo(partitionKey);
                return this;
            }

            @Override
            public StatisticValidator hasStream(String streamName) {
                assertThat(env.getSystemStream().getStream()).isEqualTo(streamName);
                return this;
            }

            @Override
            public StatisticValidator hasSystem(String systemName) {
                assertThat(env.getSystemStream().getSystem()).isEqualTo(systemName);
                return this;
            }

            @Override
            public StatisticValidator hasValidStatistics() {
                Document reads = message.getDocument(MetricFields.ENTITY_READS);
                validStatistics(reads);
                Document writes = message.getDocument(MetricFields.ENTITY_WRITES);
                validStatistics(writes);
                return this;
            }

            protected void validStatistics(Document doc) {
                Document ratesPerSecond = doc.getDocument(MetricFields.RATES_PER_SECOND);
                assertThat(ratesPerSecond.getDouble(MetricFields.RATES_1MIN)).is(NON_NEGATIVE_DOUBLE);
                assertThat(ratesPerSecond.getDouble(MetricFields.RATES_5MIN)).is(NON_NEGATIVE_DOUBLE);
                assertThat(ratesPerSecond.getDouble(MetricFields.RATES_15MIN)).is(NON_NEGATIVE_DOUBLE);
                Document timesInMillis = doc.getDocument(MetricFields.TIMES_IN_MILLIS);
                assertThat(timesInMillis.getLong(MetricFields.TIMES_MIN)).is(NON_NEGATIVE_LONG);
                assertThat(timesInMillis.getLong(MetricFields.TIMES_MAX)).is(NON_NEGATIVE_LONG);
                assertThat(timesInMillis.getDouble(MetricFields.TIMES_MEAN)).is(NON_NEGATIVE_DOUBLE);
                assertThat(timesInMillis.getDouble(MetricFields.TIMES_MEDIAN)).is(NON_NEGATIVE_DOUBLE);
                assertThat(timesInMillis.getDouble(MetricFields.TIMES_75P)).is(NON_NEGATIVE_DOUBLE);
                assertThat(timesInMillis.getDouble(MetricFields.TIMES_95P)).is(NON_NEGATIVE_DOUBLE);
                assertThat(timesInMillis.getDouble(MetricFields.TIMES_98P)).is(NON_NEGATIVE_DOUBLE);
                assertThat(timesInMillis.getDouble(MetricFields.TIMES_99P)).is(NON_NEGATIVE_DOUBLE);
                assertThat(timesInMillis.getDouble(MetricFields.TIMES_999P)).is(NON_NEGATIVE_DOUBLE);
            }

            @Override
            public StatisticValidator hasEntityCounts(int total, int creates, int updates, int deletes) {
                assertThat(message.getInteger(MetricFields.ENTITY_COUNT)).isEqualTo(total);
                Document ops = message.getDocument(MetricFields.ENTITY_OPS);
                assertThat(ops.getInteger(MetricFields.CREATES)).isEqualTo(creates);
                assertThat(ops.getInteger(MetricFields.UPDATES)).isEqualTo(updates);
                assertThat(ops.getInteger(MetricFields.DELETES)).isEqualTo(deletes);
                Document batchSizes = message.getDocument(MetricFields.ENTITY_BATCHES);
                AtomicInteger totalParts = new AtomicInteger(0);
                batchSizes.stream().forEach(field -> {
                    if (field.getName().subSequence(1, 4).equals("part")) {
                        int num = Integer.parseInt("" + field.getName().charAt(0));
                        totalParts.addAndGet(num * field.getValue().asInteger().intValue());
                    } else {
                        // There are more than 10 parts in at least one of the batches, so we can't double-check the count ...
                        totalParts.set(Integer.MIN_VALUE);
                    }
                });
                if (totalParts.get() > 0) {
                    assertThat(message.getInteger(MetricFields.ENTITY_COUNT)).isEqualTo(totalParts.get());
                }
                return this;
            }
        };
    }

    protected static Condition<Long> NON_NEGATIVE_LONG = new Condition<Long>() {
        @Override
        public boolean matches(Long value) {
            return value.longValue() >= 0L;
        }
    };

    protected static Condition<Double> NON_NEGATIVE_DOUBLE = new Condition<Double>() {
        @Override
        public boolean matches(Double value) {
            return value.doubleValue() >= 0.0d;
        }
    };

    public static interface StatisticValidator {
        StatisticValidator hasPartitionKey(Object partitionKey);

        StatisticValidator hasSystem(String systemName);

        StatisticValidator hasStream(String streamName);

        StatisticValidator hasValidStatistics();

        StatisticValidator hasEntityCounts(int total, int creates, int updates, int deletes);
    }

}
