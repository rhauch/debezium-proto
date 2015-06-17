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
public final class MetricService implements StreamTask, InitableTask, WindowableTask {

    private static final SystemStream METRICS = new SystemStream("kfka", Topic.METRICS);
    private static final String ENTITY_READ_DURATIONS = "debezium.entity.reads.time";
    private static final String ENTITY_WRITE_DURATIONS = "debezium.entity.writes.time";
    private static final String ENTITY_READ_RATE_PER_SECOND = "debezium.entity.read.rate";
    private static final String ENTITY_WRITE_RATE_PER_SECOND = "debezium.entity.write.rate";
    private static final String ENTITY_CREATED_COUNTS = "debezium.entity.created.count";
    private static final String ENTITY_UPDATED_COUNTS = "debezium.entity.updated.count";
    private static final String ENTITY_DELETED_COUNTS = "debezium.entity.deleted.count";
    private static final String ENTITY_COUNT = "debezium.entity.count";
    private static final String RESPONSE_PARTS_PREFIX = "debezium.entity.response.count.";
    
    protected static final class MetricFields {
        public static final String ENTITY_COUNT = "entity-count";
        public static final String ENTITY_OPS = "entity-ops";
        public static final String CREATES = "creates";
        public static final String UPDATES = "updates";
        public static final String DELETES = "deletes";
        public static final String ENTITY_BATCHES = "entity-batches";
        public static final String ENTITY_READS = "entity-reads";
        public static final String ENTITY_WRITES = "entity-writes";
        public static final String RATES_PER_SECOND = "rates-per-second";
        public static final String RATES_1MIN = "1min";
        public static final String RATES_5MIN = "5min";
        public static final String RATES_15MIN = "15min";
        public static final String TIMES_IN_MILLIS = "times-in-millis";
        public static final String TIMES_MIN = "min";
        public static final String TIMES_MAX = "max";
        public static final String TIMES_STD_DEV = "stddev";
        public static final String TIMES_MEAN = "mean";
        public static final String TIMES_MEDIAN = "median";
        public static final String TIMES_75P = "75p";
        public static final String TIMES_95P = "95p";
        public static final String TIMES_98P = "98p";
        public static final String TIMES_99P = "99p";
        public static final String TIMES_999P = "999p";
    }
    
    private final MetricRegistry metrics = new MetricRegistry();

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
    public void init(Config config, TaskContext context) {
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
        report.setNumber(MetricFields.ENTITY_COUNT,entities.getCount());
        report.setDocument(MetricFields.ENTITY_OPS)
              .setNumber(MetricFields.CREATES, creates.getCount())
              .setNumber(MetricFields.UPDATES, updates.getCount())
              .setNumber(MetricFields.DELETES, deletes.getCount());
        parts.write(report.setDocument(MetricFields.ENTITY_BATCHES));
        Document reads = report.setDocument(MetricFields.ENTITY_READS);
        reads.setDocument(MetricFields.RATES_PER_SECOND)
             .setNumber(MetricFields.RATES_1MIN, entityReadRatePerSecond.getOneMinuteRate())
             .setNumber(MetricFields.RATES_5MIN, entityReadRatePerSecond.getFiveMinuteRate())
             .setNumber(MetricFields.RATES_15MIN, entityReadRatePerSecond.getFifteenMinuteRate());
        reads.setDocument(MetricFields.TIMES_IN_MILLIS)
             .setNumber(MetricFields.TIMES_MIN, readDurations.getMin())
             .setNumber(MetricFields.TIMES_MAX, readDurations.getMax())
             .setNumber(MetricFields.TIMES_STD_DEV, readDurations.getStdDev())
             .setNumber(MetricFields.TIMES_MEAN, readDurations.getMean())
             .setNumber(MetricFields.TIMES_MEDIAN, readDurations.getMedian())
             .setNumber(MetricFields.TIMES_75P, readDurations.get75thPercentile())
             .setNumber(MetricFields.TIMES_95P, readDurations.get95thPercentile())
             .setNumber(MetricFields.TIMES_98P, readDurations.get98thPercentile())
             .setNumber(MetricFields.TIMES_99P, readDurations.get99thPercentile())
             .setNumber(MetricFields.TIMES_999P, readDurations.get999thPercentile());
        Document writes = report.setDocument(MetricFields.ENTITY_WRITES);
        writes.setDocument(MetricFields.RATES_PER_SECOND)
              .setNumber(MetricFields.RATES_1MIN, entityWriteRatePerSecond.getOneMinuteRate())
              .setNumber(MetricFields.RATES_5MIN, entityWriteRatePerSecond.getFiveMinuteRate())
              .setNumber(MetricFields.RATES_15MIN, entityWriteRatePerSecond.getFifteenMinuteRate());
        writes.setDocument(MetricFields.TIMES_IN_MILLIS)
              .setNumber(MetricFields.TIMES_MIN, writeDurations.getMin())
              .setNumber(MetricFields.TIMES_MAX, writeDurations.getMax())
              .setNumber(MetricFields.TIMES_STD_DEV, writeDurations.getStdDev())
              .setNumber(MetricFields.TIMES_MEAN, writeDurations.getMean())
              .setNumber(MetricFields.TIMES_MEDIAN, writeDurations.getMedian())
              .setNumber(MetricFields.TIMES_75P, writeDurations.get75thPercentile())
              .setNumber(MetricFields.TIMES_95P, writeDurations.get95thPercentile())
              .setNumber(MetricFields.TIMES_98P, writeDurations.get98thPercentile())
              .setNumber(MetricFields.TIMES_99P, writeDurations.get99thPercentile())
              .setNumber(MetricFields.TIMES_999P, writeDurations.get999thPercentile());

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
        int partCount = Message.getParts(compositeResponse,0);
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
