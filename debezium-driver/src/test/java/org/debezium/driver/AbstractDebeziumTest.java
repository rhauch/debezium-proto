/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.debezium.Testing;
import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.Entity;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.EntityType;
import org.debezium.core.component.Identifier;
import org.debezium.core.component.Schema;
import org.debezium.core.message.Batch;
import org.debezium.core.message.Patch;
import org.debezium.core.util.Stopwatch;
import org.debezium.core.util.Stopwatch.Statistics;
import org.debezium.core.util.Stopwatch.StopwatchSet;
import org.debezium.core.util.VariableLatch;
import org.debezium.driver.RandomContent.IdGenerator;
import org.junit.After;
import org.junit.Before;

import static org.fest.assertions.Assertions.assertThat;

import static org.fest.assertions.Fail.fail;

/**
 * A base for tests that use a Debezium client, with utilities for testing various operations.
 * 
 * @author Randall Hauch
 */
public abstract class AbstractDebeziumTest implements Testing {

    protected static final RandomContent RANDOM_CONTENT = RandomContent.load();
    private static final AtomicLong GENERATED_ID = new AtomicLong(0);
    protected final DatabaseId dbId = Identifier.of("my-db");
    protected final EntityType contactType = Identifier.of(dbId, "contact");
    protected String username = "jsmith";
    protected String device = "my-device";
    protected String appVersion = "2.1";
    protected Debezium.Client client;

    /**
     * Create a new Debezium.Client. This is called once at the beginning of each test.
     * 
     * @return the new client; may not be null
     */
    protected abstract Debezium.Client createClient();

    /**
     * Shut down the given Debezium.Client. This is called once at the end of each test.
     * 
     * @param client the client; never null
     * @param timeout the maximum amount of time to wait for already-submitted operations to complete before shutting down; never
     *            negative
     * @param unit the time units for {@code timeout}; never null
     */
    protected abstract void shutdownClient(Debezium.Client client, long timeout, TimeUnit unit);

    @Before
    public void beforeEach() {
        resetBeforeEachTest();
        client = createClient();
    }

    @After
    public void afterEach() {
        try {
            shutdownClient(client, 10, TimeUnit.SECONDS);
        } finally {
            client = null;
        }
    }

    /**
     * Provision a new database, invoke the given operation on the database connection, and then close the database connection.
     * 
     * @param operation the operation; may not be null
     * @throws InterruptedException if the thread is interrupted
     */
    protected void provisionAndUse(DatabaseOperation operation) throws InterruptedException {
        Database db = provisionDatabase();
        try {
            operation.runWith(db);
            if (Testing.Print.isEnabled()) {
                Testing.print(db.toString());
            }
        } finally {
            closeDatabase(db);
        }
    }

    /**
     * Provision a new database, invoke the given operation on the database connection a specified number of times, and then close
     * the database connection. Each invocation of the operation will be timed.
     * 
     * @param desc the description of the operation
     * @param repeat the number of times that the operation should be repeated; must be positive
     * @param operation the operation; may not be null
     * @return the statistics for the invocations of the operation; never null
     * @throws InterruptedException if the thread is interrupted
     */
    protected Statistics provisionAndTime(String desc, int repeat, DatabaseOperation operation) throws InterruptedException {
        return provisionAndTime(desc, null, repeat, (db, init) -> operation.runWith(db));
    }

    /**
     * Provision a new database, invoke the given operation on the database connection a specified number of times, and then close
     * the database connection. Each invocation of the operation will be timed.
     * 
     * @param desc the description of the operation
     * @param setup the one-time setup operation; may be null if not needed
     * @param repeat the number of times that the operation should be repeated; must be positive
     * @param operation the operation; may not be null
     * @return the statistics for the invocations of the operation; never null
     * @throws InterruptedException if the thread is interrupted
     */
    protected <T> Statistics provisionAndTime(String desc, Function<Database, T> setup, int repeat,
                                              DatabaseOperationAfterInitialization<T> operation) throws InterruptedException {
        Database db = provisionDatabase();
        Stopwatch sw = Stopwatch.reusable();
        Statistics timeToComplete = null;
        T initializationOutput = setup != null ? setup.apply(db) : null;
        try {
            sw.start();
            timeToComplete = time(desc, repeat, () -> {
                operation.runWith(db, initializationOutput);
                return null;
            });
            return timeToComplete;
        } finally {
            closeDatabase(db);
            sw.stop();
            if (Testing.Print.isEnabled()) {
                if (db instanceof PrintableDatabase) {
                    ((PrintableDatabase) db).print(desc + " (total time to complete all=" + sw.durations().statistics().getTotalAsString() + ")");
                }
            }
        }
    }

    /**
     * Provision a new database and assert that it is connected.
     * 
     * @return the new database; never null
     */
    protected Database provisionDatabase() {
        Database db = client.provision(dbId, username, device, appVersion);
        assertThat(db.isConnected()).isTrue();
        return wrap(db);
    }

    /**
     * Connect to an existing database and assert that it is connected.
     * 
     * @return the new database; never null
     */
    protected Database connectToDatabase() {
        Database db = client.connect(dbId, username, device, appVersion);
        assertThat(db.isConnected()).isTrue();
        return wrap(db);
    }

    /**
     * Close the given database.
     * 
     * @param db the database; never null
     */
    protected void closeDatabase(Database db) {
        db.close();
        assertThat(db.isConnected()).isFalse();
    }

    /**
     * Create an initialization function that will create the specified number of new entities of the given entity type.
     * 
     * @param count the number of new entities; must be positive
     * @param type the type of entities to create; may not be null
     * @return the initialization function; never null
     */
    protected Function<Database, Collection<EntityId>> createEntities(int count, EntityType type) {
        Batch<EntityId> batch = RANDOM_CONTENT.createGenerator().generateBatch(generateIds(count, 0, type));
        Set<EntityId> ids = batch.stream().map(Patch::target).collect(Collectors.toSet());
        return db -> {
            db.changeEntities(batch, output -> {
                if (output.failed()) {
                    fail("Unable to create entities: " + output.failureReason());
                }
            });
            return ids;
        };
    }

    /**
     * Create the specified number of new entities of the given entity type.
     * 
     * @param db the database; never null
     * @param count the number of new entities; must be positive
     * @param type the type of entities to create; may not be null
     * @return the set of entity IDs; never null
     */
    protected List<EntityId> createEntities(Database db, int count, EntityType type) {
        Batch<EntityId> batch = RANDOM_CONTENT.createGenerator().generateBatch(generateIds(count, 0, type));
        List<EntityId> ids = batch.stream().map(Patch::target).collect(Collectors.toList());
        List<EntityId> changedIds = new ArrayList<>(ids);
        assertThat(ids.size()).isEqualTo(count);
        assertThat(changedIds.size()).isEqualTo(count);
        assertThat(changedIds).isEqualTo(ids);
        CountDownLatch latch = new CountDownLatch(1);
        db.changeEntities(batch, output -> {
            if (output.failed()) {
                if (count == 1) {
                    fail("Unable to create 1 entity: " + output.failureReason());
                } else {
                    fail("Unable to create " + count + " entities: " + output.failureReason());
                }
            } else {
                output.result().forEach(change -> {
                    // The change should succeed and each
                    assertThat(change.succeeded()).isTrue();
                    assertThat(changedIds.remove(change.target().id())).isTrue();
                });
            }
            latch.countDown();
        });
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail("Failed to wait for changedEntities to complete");
        }
        assertThat(changedIds.isEmpty()).isTrue();
        assertThat(ids.size()).isEqualTo(count);
        return ids;
    }

    private IdGenerator generateIds(int editCount, int removeCount, EntityType type) {
        return new IdGenerator() {
            @Override
            public EntityId[] generateEditableIds() {
                return generateIds(editCount, type);
            }

            @Override
            public EntityId[] generateRemovableIds() {
                return generateIds(removeCount, type);
            }

            private EntityId[] generateIds(int count, EntityType type) {
                if (count <= 0) return null;
                EntityId[] ids = new EntityId[count];
                for (int i = 0; i != count; ++i) {
                    ids[i] = Identifier.of(type, Long.toString(GENERATED_ID.incrementAndGet()));
                }
                return ids;
            }
        };
    }

    @FunctionalInterface
    public static interface DatabaseOperation {
        /**
         * Perform an operation that use the given database.
         * 
         * @param db the database; never null
         * @throws InterruptedException if the thread is interrupted
         */
        public void runWith(Database db) throws InterruptedException;
    }

    @FunctionalInterface
    public static interface DatabaseOperationAfterInitialization<T> {
        /**
         * Perform an operation that use the given database.
         * 
         * @param db the database; never null
         * @param setupOutput the output of the initialization; may be null
         * @throws InterruptedException if the thread is interrupted
         */
        public void runWith(Database db, T setupOutput) throws InterruptedException;
    }

    protected static interface PrintableDatabase extends Database {
        public void print(String desc);
    }

    /**
     * Wrap an existing database connection so that {@link Database#close()} will wait for all submitted requests to complete.
     * 
     * @param db the database; may not be null
     * @return the wrapped database; never null
     */
    private Database wrap(Database db) {
        VariableLatch latch = VariableLatch.create();
        StopwatchSet readSchemaRequestTimers = Stopwatch.multiple();
        StopwatchSet readSchemaResponseTimers = Stopwatch.multiple();
        StopwatchSet readEntitiesRequestTimers = Stopwatch.multiple();
        StopwatchSet readEntitiesResponseTimers = Stopwatch.multiple();
        StopwatchSet changeEntitiesRequestTimers = Stopwatch.multiple();
        StopwatchSet changeEntitiesResponseTimers = Stopwatch.multiple();

        return new PrintableDatabase() {
            @Override
            public DatabaseId databaseId() {
                return db.databaseId();
            }

            @Override
            public boolean isConnected() {
                return db.isConnected();
            }

            @Override
            public Completion readSchema(OutcomeHandler<Schema> handler) {
                latch.countUp();
                latch.countUp();
                Stopwatch requestTimer = readSchemaRequestTimers.create().start();
                Stopwatch responseTimer = readSchemaResponseTimers.create().start();
                try {
                    return db.readSchema(outcome -> {
                        responseTimer.stop();
                        try {
                            handler.handle(outcome);
                        } finally {
                            latch.countDown();
                        }
                    });
                } finally {
                    requestTimer.stop();
                    latch.countDown();
                }
            }

            @Override
            public Completion readEntities(Iterable<EntityId> entityIds, OutcomeHandler<Stream<Entity>> handler) {
                latch.countUp();
                latch.countUp();
                Stopwatch requestTimer = readEntitiesRequestTimers.create().start();
                Stopwatch responseTimer = readEntitiesResponseTimers.create().start();
                try {
                    return db.readEntities(entityIds, outcome -> {
                        responseTimer.stop();
                        try {
                            handler.handle(outcome);
                        } finally {
                            latch.countDown();
                        }
                    });
                } finally {
                    requestTimer.stop();
                    latch.countDown();
                }
            }

            @Override
            public Completion changeEntities(Batch<EntityId> batch, OutcomeHandler<Stream<Change<EntityId, Entity>>> handler) {
                latch.countUp();
                latch.countUp();
                Stopwatch requestTimer = changeEntitiesRequestTimers.create().start();
                Stopwatch responseTimer = changeEntitiesResponseTimers.create().start();
                try {
                    return db.changeEntities(batch, outcome -> {
                        responseTimer.stop();
                        try {
                            handler.handle(outcome);
                        } finally {
                            latch.countDown();
                        }
                    });
                } finally {
                    requestTimer.stop();
                    latch.countDown();
                }
            }

            protected void awaitCompletion() {
                try {
                    // Give each submitted operation a chance to complete ...
                    readSchemaRequestTimers.await(10, TimeUnit.SECONDS);
                    readSchemaResponseTimers.await(10, TimeUnit.SECONDS);
                    readEntitiesRequestTimers.await(10, TimeUnit.SECONDS);
                    readEntitiesResponseTimers.await(10, TimeUnit.SECONDS);
                    changeEntitiesRequestTimers.await(10, TimeUnit.SECONDS);
                    changeEntitiesResponseTimers.await(10, TimeUnit.SECONDS);
                    latch.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    fail("Unexpectedly interrupted: " + e.getLocalizedMessage());
                }
            }

            @Override
            public void close() {
                awaitCompletion();
                db.close();
            }

            @Override
            public void print(String desc) {
                if (Testing.Print.isEnabled()) {
                    awaitCompletion();
                    Testing.print("Times to " + desc);
                    Testing.print("  [read schemas]      submit request " + readSchemaRequestTimers.statistics());
                    Testing.print("                    receive response " + readSchemaResponseTimers.statistics());
                    Testing.print("  [read entities]     submit request " + readEntitiesRequestTimers.statistics());
                    Testing.print("                    receive response " + readEntitiesResponseTimers.statistics());
                    Testing.print("  [change entities]   submit request " + changeEntitiesRequestTimers.statistics());
                    Testing.print("                    receive response " + changeEntitiesResponseTimers.statistics());
                }
            }
        };
    }
}
