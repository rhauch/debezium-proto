/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.example;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.Entity;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.Schema;
import org.debezium.core.doc.Document;
import org.debezium.core.message.Batch;
import org.debezium.core.message.Patch;
import org.debezium.driver.Database;
import org.debezium.driver.Debezium;
import org.debezium.example.RandomContent.ContentGenerator;

/**
 * @author Randall Hauch
 */
public class MockDatabase implements Database {

    public static Debezium.Client createClient( RandomContent content ) {
        return new MockClient(content);
    }
    
    private static class MockClient implements Debezium.Client {
        private final RandomContent content;
        private MockClient( RandomContent content ) {
            this.content = content;
        }

        @Override
        public Database connect(DatabaseId id, String username, String device, String appVersion) {
            return new MockDatabase(id, content.createGenerator() );
        }
        
        @Override
        public Database connect(DatabaseId id, String username, String device, String appVersion, long timeout, TimeUnit unit) {
            return new MockDatabase(id, content.createGenerator() );
        }

        @Override
        public Database provision(DatabaseId id, String username, String device, String appVersion) {
            return new MockDatabase(id, content.createGenerator() );
        }
        
        @Override
        public Database provision(DatabaseId id, String username, String device, String appVersion, long timeout, TimeUnit unit) {
            return new MockDatabase(id, content.createGenerator() );
        }

        @Override
        public void shutdown(long timeout, TimeUnit unit) {
            // do nothing
        }
        
    }
    
    private final DatabaseId dbId;
    private final ContentGenerator generator;
    
    private MockDatabase( DatabaseId dbId, ContentGenerator generator ) {
        this.dbId = dbId;
        this.generator = generator;
    }

    @Override
    public DatabaseId databaseId() {
        return dbId;
    }

    @Override
    public Completion readSchema(OutcomeHandler<Schema> handler) {
        return new Completed();
    }

    @Override
    public Completion readEntities(Iterable<EntityId> entityIds, OutcomeHandler<Stream<Entity>> handler) {
        List<Entity> entities = new ArrayList<>();
        for ( EntityId id : entityIds ) {
            entities.add(generator.generateEntity(id));
        }
        handler.handle(success(entities.stream()));
        return new Completed();
    }

    @Override
    public Completion changeEntities(Batch<EntityId> batch, OutcomeHandler<Stream<Change<EntityId, Entity>>> handler) {
        List<Change<EntityId,Entity>> changes = new ArrayList<>();
        for ( Patch<EntityId> patch : batch ) {
            Document doc = Document.create();
            ChangeStatus status = patch.apply(doc,(op)->{}) ? ChangeStatus.OK : ChangeStatus.PATCH_FAILED;
            Entity entity = Entity.with(patch.target(), doc);
            Change<EntityId,Entity> change = new Change<EntityId,Entity>() {

                @Override
                public Patch<EntityId> patch() {
                    return patch;
                }

                @Override
                public Entity target() {
                    return entity;
                }

                @Override
                public EntityId id() {
                    return patch.target();
                }

                @Override
                public ChangeStatus status() {
                    return status;
                }

                @Override
                public Stream<String> failureReasons() {
                    return Stream.empty();
                }
            };
            changes.add(change);
        }
        handler.handle(success(changes.stream()));
        return new Completed();
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Override
    public void close() {
        // do nothing
    }
    
    private static class Completed implements Completion {

        @Override
        public boolean isComplete() {
            return true;
        }

        @Override
        public void await() throws InterruptedException {
            // do nothing
        }

        @Override
        public void await(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
            // do nothing
        }
    }
    
    protected static <T> Outcome<T> success( T result ) {
        return new Outcome<T>() {

            @Override
            public org.debezium.driver.Database.Outcome.Status status() {
                return Status.OK;
            }

            @Override
            public String failureReason() {
                return null;
            }

            @Override
            public T result() {
                return result;
            }
            
        };
    }
}
