/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.example;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.debezium.core.component.Entity;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.EntityType;
import org.debezium.core.component.Identifier;
import org.debezium.core.message.Batch;
import org.debezium.driver.Database;
import org.debezium.driver.Database.Change;
import org.debezium.driver.Database.Completion;
import org.debezium.driver.Database.Outcome;
import org.debezium.example.RandomContent.ContentGenerator;

import com.codahale.metrics.Meter;

/**
 * @author Randall Hauch
 *
 */
public class Client implements Callable<Results> {

    private final String name;
    private final Database db;
    private final long numRequests;
    private final ContentGenerator contentGenerator;
    private final IdGenerator idGenerator;
    private final Meter meter;
    private long numBatchSuccesses;
    private long numBatchFailures;
    private long numBatchTimeoutFailures;
    private long numPatchSuccesses;
    private long numPatchRejections;
    private long numPatchFailedNoEntities;

    public Client(String name, Database db, ContentGenerator generator, long numRequests, EntityType type, Meter meter) {
        this.name = name;
        this.db = db;
        this.numRequests = numRequests;
        this.contentGenerator = generator;
        this.idGenerator = new IdGenerator(name, 4, type);
        this.meter = meter;
    }

    @Override
    public Results call() {
        for (int i = 0; i != numRequests; ++i) {
            if ( i % 100000 == 0 ) LoadApp.printVerbose("Client " + name + " generating batch " + i );
            try {
                Batch<EntityId> batch = contentGenerator.generateBatch(idGenerator);
                Completion result = db.changeEntities(batch, this::handleUpdate);
                if (result != null && !result.isComplete()) {
                    result.await(10, TimeUnit.SECONDS);
                    if (!result.isComplete()) {
                        ++numBatchTimeoutFailures;
                    }
                }
            } catch (TimeoutException e) {
                ++numBatchTimeoutFailures;
                LoadApp.printVerbose("Client " + name + " received timeout waiting for result of batch " + i );
            } catch (InterruptedException e) {
                LoadApp.printVerbose("Client " + name + " interrupted");
                Thread.interrupted();
            } finally {
                meter.mark();
            }
            if (numBatchFailures > 10) break;
        }
        return new Results(numBatchSuccesses, numBatchFailures, numBatchTimeoutFailures, numPatchSuccesses, numPatchRejections,
                numPatchFailedNoEntities);
    }

    protected void handleUpdate(Outcome<Stream<Change<EntityId, Entity>>> outcome) {
        if (outcome.succeeded()) {
            ++numBatchSuccesses;
            outcome.result().forEach(change -> {
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
            });
        } else {
            // Unable to even submit the batch, so handle the error. Perhaps the system is not available, or
            // our request was poorly formed (e.g., was empty). The best way to handle this is to do different
            // things based upon the exception type ...
            // String reason = outcome.failureReason();
            ++numBatchFailures;
        }
    }

    private static class IdGenerator implements RandomContent.IdGenerator {
        private static EntityId[] EMPTY;
        private final EntityId[] ids;
        private final int count;
        private final EntityType type;
        private final String idPrefix;

        protected IdGenerator(String idPrefix, int numEditedIds, EntityType entityType) {
            this.ids = new EntityId[numEditedIds];
            this.count = numEditedIds;
            this.type = entityType;
            this.idPrefix = idPrefix + "-";
        }

        @Override
        public EntityId[] generateEditableIds() {
            for (int i = 0; i != count; ++i) {
                ids[i] = Identifier.of(type, idPrefix + Integer.toString(count));
            }
            return ids;
        }

        @Override
        public EntityId[] generateRemovableIds() {
            return EMPTY;
        }
    }

}
