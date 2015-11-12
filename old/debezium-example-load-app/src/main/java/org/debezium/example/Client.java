/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.example;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.debezium.core.component.EntityType;
import org.debezium.driver.BatchResult;
import org.debezium.driver.Debezium;
import org.debezium.driver.DebeziumTimeoutException;
import org.debezium.driver.EntityChange;
import org.debezium.driver.RandomContent.ContentGenerator;
import org.debezium.driver.SessionToken;

import com.codahale.metrics.Meter;

/**
 * @author Randall Hauch
 *
 */
public class Client implements Callable<Results> {

    private final String name;
    private final Debezium driver;
    private final SessionToken session;
    private final long numRequests;
    private final ContentGenerator contentGenerator;
    private final EntityType type;
    private final Meter meter;
    private long numBatchSuccesses;
    private long numBatchFailures;
    private long numBatchTimeoutFailures;
    private long numPatchSuccesses;
    private long numPatchRejections;
    private long numPatchFailedNoEntities;

    public Client(String name, Debezium driver, SessionToken session, ContentGenerator generator, long numRequests, EntityType type,
            Meter meter) {
        this.name = name;
        this.driver = driver;
        this.session = session;
        this.numRequests = numRequests;
        this.contentGenerator = generator;
        this.type = type;
        this.meter = meter;
    }

    @Override
    public Results call() {
        for (int i = 0; i != numRequests; ++i) {
            if (i % 100000 == 0) LoadApp.printVerbose("Client " + name + " generating batch " + i);
            try {
                BatchResult batchResult = contentGenerator.addToBatch(driver.batch(), 4, 0, type)
                                                          .submit(session, 10, TimeUnit.SECONDS);
                ++numBatchSuccesses;
                batchResult.changeStream().forEach(this::handleUpdate);
            } catch (DebeziumTimeoutException e) {
                ++numBatchTimeoutFailures;
                LoadApp.printVerbose("Client " + name + " received timeout waiting for result of batch " + i);
            } catch (Throwable t) {
                ++numBatchFailures;
                LoadApp.printVerbose("Client " + name + " could not submit batch: " + t);
            } finally {
                meter.mark();
            }
            if (numBatchFailures > 10) break;
        }
        return new Results(numBatchSuccesses, numBatchFailures, numBatchTimeoutFailures, numPatchSuccesses, numPatchRejections,
                numPatchFailedNoEntities);
    }

    protected void handleUpdate(EntityChange change) {
        if (change.succeeded()) {
            // We successfully changed this entity by applying our patch ...
            ++numPatchSuccesses;
        } else {
            // Our patch for this entity could not be applied ...
            switch (change.status()) {
                case OK:
                    // This would have been a success
                    break;
                case DOES_NOT_EXIST:
                    // The entity was removed by someone else, so we'll delete it locally ...
                    ++numPatchFailedNoEntities;
                    break;
                case PATCH_FAILED:
                    // We put preconditions into our patch that were not satisfied, so perhaps we
                    // should rebuild a new patch with the latest representation of the target.
                    ++numPatchRejections;
                    break;
            }
        }
    }
}
