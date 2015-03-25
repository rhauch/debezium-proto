/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;
import org.debezium.core.annotation.NotThreadSafe;
import org.debezium.core.doc.Document;
import org.debezium.core.message.Message;
import org.debezium.core.message.Topic;

import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;

/**
 * A service (or task in Samza parlance) that monitors the {@value Topic#COMPLETE_RESPONSES} stream and calculates performance
 * metrics and outputs them every second.
 * <p>
 * This service consumes the {@value Topic#COMPLETE_RESPONSES} topic from the "debezium" system.
 * <p>
 * This service produces a message for each metric window update on the Topic#METRICS_MINUTES} topic from the "debezium" system.
 * <p>
 * This service uses the Dropwizard <a href="https://dropwizard.github.io/metrics/3.1.0/about/release-notes/">Metrics library</a>
 * internally to compute the interesting metric values. Note that Kafka internally uses an older version (2.x), but Metrics 3.x
 * was moved to a different package structure that enables both 2.x and 3.x to be used at the same time in an application with a
 * simple classloader.
 * 
 * @author Randall Hauch
 */
@NotThreadSafe
public class MetricService implements StreamTask, InitableTask, WindowableTask {

    private static final SystemStream METRICS = new SystemStream("debezium", Topic.METRICS);
    private static final String ENTITY_READ_DURATIONS = "debezium.entity.reads.time";
    private static final String ENTITY_WRITE_DURATIONS = "debezium.entity.writes.time";
    private static final String ENTITY_READ_RATE_PER_SECOND = "debezium.entity.read.rate";
    private static final String ENTITY_WRITE_RATE_PER_SECOND = "debezium.entity.write.rate";
    private static final String ENTITY_CREATED_COUNTS = "debezium.entity.created.count";
    private static final String ENTITY_UPDATED_COUNTS = "debezium.entity.updated.count";
    private static final String ENTITY_DELETED_COUNTS = "debezium.entity.deleted.count";
    private static final String ENTITY_COUNT = "debezium.entity.count";
    private static final String RESPONSE_PARTS_PREFIX = "debezium.entity.response.count.";
    
    private static final MetricRegistry metrics = new MetricRegistry();

    private Histogram entityReadDurations;
    private Histogram entityWriteDurations;
    private Meter entityReadRatePerSecond;
    private Meter entityWriteRatePerSecond;
    private PartsCounter parts;
    private Counter creates;
    private Counter updates;
    private Counter deletes;
    private Counter entities;

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        // Register all of the metrics that we'll use ...
        entityReadDurations = metrics.register(ENTITY_READ_DURATIONS, new Histogram(new ExponentiallyDecayingReservoir()));
        entityWriteDurations = metrics.histogram(ENTITY_WRITE_DURATIONS);
        entityReadRatePerSecond = metrics.meter(ENTITY_READ_RATE_PER_SECOND);
        entityWriteRatePerSecond = metrics.meter(ENTITY_WRITE_RATE_PER_SECOND);
        parts = new PartsCounter(metrics, RESPONSE_PARTS_PREFIX);
        creates = metrics.counter(ENTITY_CREATED_COUNTS);
        updates = metrics.counter(ENTITY_UPDATED_COUNTS);
        deletes = metrics.counter(ENTITY_DELETED_COUNTS);
        entities = metrics.counter(ENTITY_COUNT);
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        // Collect the metrics and write them out ...
        Snapshot readDurations = entityReadDurations.getSnapshot();
        Snapshot writeDurations = entityWriteDurations.getSnapshot();

        Document report = Document.create();
        report.setNumber("entity-count",entities.getCount());
        report.setDocument("entity-ops")
              .setNumber("creates", creates.getCount())
              .setNumber("updates", updates.getCount())
              .setNumber("deletes", deletes.getCount());
        parts.write(report.setDocument("entity-batches"));
        Document reads = report.setDocument("entity-reads");
        reads.setDocument("rates-per-second")
             .setNumber("1min", entityReadRatePerSecond.getOneMinuteRate())
             .setNumber("5min", entityReadRatePerSecond.getFiveMinuteRate())
             .setNumber("15min", entityReadRatePerSecond.getFifteenMinuteRate());
        reads.setDocument("times-in-millis")
             .setNumber("min", readDurations.getMin())
             .setNumber("max", readDurations.getMax())
             .setNumber("stddev", readDurations.getStdDev())
             .setNumber("mean", readDurations.getMean())
             .setNumber("median", readDurations.getMedian())
             .setNumber("75p", readDurations.get75thPercentile())
             .setNumber("95p", readDurations.get95thPercentile())
             .setNumber("98p", readDurations.get98thPercentile())
             .setNumber("99p", readDurations.get99thPercentile())
             .setNumber("999p", readDurations.get999thPercentile());
        Document writes = report.setDocument("entity-writes");
        writes.setDocument("rates-per-second")
              .setNumber("1min", entityWriteRatePerSecond.getOneMinuteRate())
              .setNumber("5min", entityWriteRatePerSecond.getFiveMinuteRate())
              .setNumber("15min", entityWriteRatePerSecond.getFifteenMinuteRate());
        writes.setDocument("times-in-millis")
              .setNumber("min", writeDurations.getMin())
              .setNumber("max", writeDurations.getMax())
              .setNumber("stddev", writeDurations.getStdDev())
              .setNumber("mean", writeDurations.getMean())
              .setNumber("median", writeDurations.getMedian())
              .setNumber("75p", writeDurations.get75thPercentile())
              .setNumber("95p", writeDurations.get95thPercentile())
              .setNumber("98p", writeDurations.get98thPercentile())
              .setNumber("99p", writeDurations.get99thPercentile())
              .setNumber("999p", writeDurations.get999thPercentile());

        collector.send(new OutgoingMessageEnvelope(METRICS, report));

        // Reset the counters that don't exponentially decay on their own ...
        parts.reset();
        creates.dec(creates.getCount());
        updates.dec(updates.getCount());
        deletes.dec(deletes.getCount());
    }

    @Override
    public void process(IncomingMessageEnvelope env, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        Document compositeResponse = (Document) env.getMessage();

        // Process the composite response ...
        int partCount = Message.getParts(compositeResponse);
        if (partCount > 0) {
            parts.record(partCount);
            Message.forEachPartialResponse(compositeResponse, (id, partialNumber, request, response) -> {
                long duration = Message.getDurationInMillis(response);
                if (Message.isReadOnly(response)) {
                    entityReadRatePerSecond.mark();
                    entityReadDurations.update(duration);
                } else {
                    entityWriteRatePerSecond.mark();
                    entityWriteDurations.update(duration);
                    switch(Message.determineAction(response)) {
                        case CREATED:
                            creates.inc();
                            entities.inc();
                            break;
                        case UPDATED:
                            updates.inc();
                            break;
                        case DELETED:
                            deletes.inc();
                            entities.dec();
                            break;
                    }
                }
            });
        }
    }
}
