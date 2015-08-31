/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.EntityType;
import org.debezium.core.component.Identifier;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.Value;
import org.debezium.core.message.Batch;
import org.debezium.core.message.Message;
import org.debezium.core.message.Patch;
import org.debezium.core.message.Patch.Editor;
import org.debezium.core.message.Topic;
import org.debezium.driver.EntityChange.ChangeStatus;
import org.debezium.driver.SecurityProvider.CompositeAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Randall Hauch
 *
 */
final class DbzDriver implements Debezium {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Configuration config;
    private final Environment env;
    private final DbzNode node;
    private final DbzDatabases databases;
    private final DbzPartialResponses partialResponses;
    private final Clock clock = Clock.system();

    DbzDriver(Configuration config, Environment env) {
        this.config = config;
        this.env = env;
        this.node = new DbzNode(this.config, env);
        this.partialResponses = new DbzPartialResponses();
        this.databases = new DbzDatabases(this.partialResponses);
        this.node.add(this.databases, this.partialResponses);
    }
    
    @Override
    public Configuration getConfiguration() {
        return config;
    }

    public DbzDriver start() {
        node.start();
        return this;
    }

    private void logConnection(SessionToken token, String[] databaseIds, long durationInNanos, String action) {
        env.getSecurity().info(token, (username, device, appVersion) -> {
            // TODO: record information in metrics
                           });
    }

    private void logUsage(SessionToken token, String databaseId, long durationInNanos, String action) {
        env.getSecurity().info(token, (username, device, appVersion) -> {
            // TODO: record information in metrics
                           });
    }

    private void logUsage(SessionToken token, String databaseId, long durationInNanos, String action, String field, Object value) {
        env.getSecurity().info(token, (username, device, appVersion) -> {
            // TODO: record information in metrics
                           });
    }

    private void logUsage(SessionToken token, String databaseId, long durationInNanos, String action, String field1, Object value1,
                          String field2, Object value2) {
        env.getSecurity().info(token, (username, device, appVersion) -> {
            // TODO: record information in metrics
                           });
    }

    private long duration(long start) {
        return clock.currentTimeInNanos() - start;
    }
    
    private DebeziumClientException notRunning() {
        return new DebeziumClientException("The Debezium driver is not running");
    }

    @Override
    public SessionToken connect(String username, String device, String appVersion, String... databaseIds) {
        return node.whenRunning(() -> {
            long start = clock.currentTimeInNanos();
            SessionToken token = null;
            Set<String> existingDbIds = databases.existing(databaseIds);
            token = env.getSecurity().authenticate(username, device, appVersion, existingDbIds, databaseIds);
            if (token == null) {
                throw new DebeziumAuthorizationException("Unable to authenticate user '" + username + "' for databases: " + databaseIds);
            }
            logConnection(token, databaseIds, duration(start), "connect");
            return token;
        }).orElseThrow(this::notRunning);
    }

    @Override
    public void provision(SessionToken adminToken, String databaseId, long timeout, TimeUnit unit) {
        node.whenRunning(() -> {
            long start = clock.currentTimeInNanos();
            String username = env.getSecurity().canAdminister(adminToken, databaseId);
            if (username == null) {
                throw new DebeziumAuthorizationException("Unable to read schema for database '" + databaseId + "'");
            }
            databases.provision(username, Identifier.of(databaseId), timeout, unit);
            logUsage(adminToken, databaseId, duration(start), "provision");
            return Boolean.TRUE;
        }).orElseThrow(this::notRunning);
    }

    @Override
    public Schema readSchema(SessionToken token, String databaseId, long timeout, TimeUnit unit) {
        return node.whenRunning(() -> {
            long start = clock.currentTimeInNanos();
            String username = env.getSecurity().canRead(token, databaseId);
            if (username == null) {
                throw new DebeziumAuthorizationException("Unable to read schema for database '" + databaseId + "'");
            }
            Schema schema = databases.readSchema(databaseId);
            logUsage(token, databaseId, duration(start), "readSchema");
            return schema;
        }).orElseThrow(this::notRunning);
    }

    @Override
    public Entity readEntity(SessionToken token, EntityId entityId, long timeout, TimeUnit unit) {
        return node.whenRunning(() -> {
            long start = clock.currentTimeInNanos();
            // Check the privilege first ...
            DatabaseId dbId = entityId.databaseId();
            String databaseName = dbId.asString();
            String username = env.getSecurity().canRead(token, databaseName);
            if (username == null) {
                throw new DebeziumAuthorizationException("Unable to read entity '" + entityId + "'");
            }
            logger.debug("Attempting to read entity '{}'", entityId);
            return partialResponses.submit(Entity.class, requestId -> {
                logger.trace("Attempting to submit request to read entity '{}'", entityId);
                Document request = Patch.read(entityId).asDocument();
                Message.addHeaders(request, requestId.getClientId(), requestId.getRequestNumber(), username);
                if (!node.send(Topic.ENTITY_PATCHES, entityId.asString(), request)) {
                    throw new DebeziumClientException("Unable to send request to read entity '" + entityId + "'");
                }
            }).onResponse(timeout, unit, response -> {
                logger.trace("Received response from reading entity '{}'", entityId);
                EntityId id = Message.getEntityId(response);
                Document representation = Message.getAfter(response);
                if (representation != null) {
                    logUsage(token, databaseName, duration(start), "readEntity", "found", true);
                    logger.trace("Successfully read entity '{}'", entityId);
                } else {
                    logUsage(token, databaseName, duration(start), "readEntity", "found", false);
                    logger.trace("Unable to find entity '{}'", entityId);
                }
                return new DbzEntity(id, representation);
            }).onTimeout(() -> {
                throw new DebeziumTimeoutException("The request to read entity '" + entityId + "' timed out");
            });
        }).orElseThrow(this::notRunning);
    }

    @Override
    public EntityChange changeEntity(SessionToken token, Patch<EntityId> patch, long timeout, TimeUnit unit) {
        return node.whenRunning(() -> {
            long start = clock.currentTimeInNanos();
            // Check the privilege first ...
            EntityId entityId = patch.target();
            DatabaseId dbId = entityId.databaseId();
            String databaseName = dbId.asString();
            String username = env.getSecurity().canWrite(token, databaseName);
            if (username == null) {
                throw new DebeziumAuthorizationException("Unable to change entity '" + entityId + "'");
            }
            logger.debug("Attempting to change entity '{}' with patch: {}", entityId, patch);
            return partialResponses.submit(EntityChange.class, requestId -> {
                logger.trace("Attempting to submit request to change entity '{}'", entityId);
                Document request = patch.asDocument();
                Message.addHeaders(request, requestId.getClientId(), requestId.getRequestNumber(), username);
                if (!node.send(Topic.ENTITY_PATCHES, entityId.asString(), request)) {
                    throw new DebeziumClientException("Unable to send request to change entity '" + entityId + "'");
                }
            }).onResponse(timeout, unit, response -> {
                logger.trace("Received response from changing entity '{}'", entityId);
                EntityId id = Message.getEntityId(response);
                Document representation = Message.getAfterOrBefore(response);
                ChangeStatus status = null;
                Collection<String> failureReasons = null;
                switch (Message.getStatus(response)) {
                    case SUCCESS:
                        status = ChangeStatus.OK;
                        failureReasons = Message.getFailureReasons(response);
                        logUsage(token, databaseName, duration(start), "changeEntity", "found", true, "changed", true);
                        logger.trace("Successfully changed entity '{}'", id);
                        break;
                    case PATCH_FAILED:
                        status = ChangeStatus.PATCH_FAILED;
                        failureReasons = Message.getFailureReasons(response);
                        logUsage(token, databaseName, duration(start), "changeEntity", "found", true, "changed", false);
                        logger.trace("Unable to apply patch to change entity '{}'", id);
                        break;
                    case DOES_NOT_EXIST:
                        status = ChangeStatus.DOES_NOT_EXIST;
                        failureReasons = Message.getFailureReasons(response);
                        logUsage(token, databaseName, duration(start), "changeEntity", "found", false, "changed", false);
                        logger.trace("Unable to find entity '{}'", id);
                        break;
                }
                Entity entity = new DbzEntity(id, representation);
                return new DbzEntityChange(patch, entity, status, failureReasons);
            }).onTimeout(() -> {
                throw new DebeziumTimeoutException("The request to change entity '" + entityId + "' timed out");
            });
        }).orElseThrow(this::notRunning);
    }

    @Override
    public boolean destroyEntity(SessionToken token, EntityId entityId, long timeout, TimeUnit unit) {
        return node.whenRunning(() -> {
            long start = clock.currentTimeInNanos();
            // Check the privilege first ...
            DatabaseId dbId = entityId.databaseId();
            String databaseName = dbId.asString();
            String username = env.getSecurity().canWrite(token, databaseName);
            if (username == null) {
                throw new DebeziumAuthorizationException("Unable to destroy entity '" + entityId + "'");
            }
            logger.debug("Attempting to destroy entity '{}'", entityId);
            return partialResponses.submit(Boolean.class, requestId -> {
                logger.trace("Attempting to submit request to destroy entity '{}'", entityId);
                Document request = Patch.destroy(entityId).asDocument();
                Message.addHeaders(request, requestId.getClientId(), requestId.getRequestNumber(), username);
                if (!node.send(Topic.ENTITY_PATCHES, entityId.asString(), request)) {
                    throw new DebeziumClientException("Unable to send request to read entity '" + entityId + "'");
                }
            }).onResponse(timeout, unit, response -> {
                logger.trace("Received response from destroying entity '{}'", entityId);
                EntityId id = Message.getEntityId(response);
                if (Message.getBefore(response) != null) {
                    logUsage(token, databaseName, duration(start), "destroyEntity", "succeed", true);
                    logger.trace("Successfully destroyed entity '{}'", id);
                    return true;
                }
                logUsage(token, databaseName, duration(start), "destroyEntity", "succeed", false);
                logger.trace("Unable to find and destroy entity '{}'", id);
                return false;
            }).onTimeout(() -> {
                throw new DebeziumTimeoutException("The request to destroy '" + entityId + "' timed out");
            });
        }).orElseThrow(this::notRunning);
    }

    @Override
    public BatchBuilder batch() {
        return new BatchBuilder() {
            private final Batch.Builder<EntityId> batchBuilder = Batch.create();

            @Override
            public BatchBuilder readEntity(EntityId entityId) {
                batchBuilder.read(entityId);
                return this;
            }

            @Override
            public BatchBuilder changeEntity(Patch<EntityId> patch) {
                batchBuilder.patch(patch);
                return this;
            }
            
            @Override
            public Editor<BatchBuilder> createEntity(EntityType entityType) {
                return changeEntity(Identifier.newEntity(entityType));
            }

            @Override
            public Editor<BatchBuilder> changeEntity(EntityId entityId) {
                Patch.Editor<Patch<EntityId>> editor = Patch.edit(entityId);
                BatchBuilder builder = this;
                return new Editor<BatchBuilder>() {
                    @Override
                    public Editor<BatchBuilder> add(String path, Value value) {
                        editor.add(path, value);
                        return this;
                    }
                    @Override
                    public Editor<BatchBuilder> copy(String fromPath, String toPath) {
                        editor.copy(fromPath, toPath);
                        return this;
                    }
                    @Override
                    public Editor<BatchBuilder> increment(String path, Number increment) {
                        editor.increment(path, increment);
                        return this;
                    }
                    @Override
                    public Editor<BatchBuilder> move(String fromPath, String toPath) {
                        editor.move(fromPath, toPath);
                        return this;
                    }
                    @Override
                    public Editor<BatchBuilder> remove(String path) {
                        editor.remove(path);
                        return this;
                    }
                    @Override
                    public Editor<BatchBuilder> replace(String path, Value newValue) {
                        editor.replace(path, newValue);
                        return this;
                    }
                    @Override
                    public Editor<BatchBuilder> require(String path, Value expectedValue) {
                        editor.require(path, expectedValue);
                        return this;
                    }
                    @Override
                    public BatchBuilder end() {
                        editor.endIfChanged().ifPresent(builder::changeEntity);
                        return builder;
                    }
                    @Override
                    public Optional<BatchBuilder> endIfChanged() {
                        editor.endIfChanged().ifPresent(builder::changeEntity);
                        return Optional.of(builder);
                    }
                };
            }

            @Override
            public BatchBuilder destroyEntity(EntityId entityId) {
                batchBuilder.remove(entityId);
                return this;
            }
            
            @Override
            public BatchResult submit(SessionToken token, long timeout, TimeUnit unit) {
                return submitBatch(token, batchBuilder.build(), timeout, unit); // resets batch builder each time
            }
        };
    }

    private BatchResult submitBatch(SessionToken token, Batch<EntityId> batch, long timeout, TimeUnit unit) {
        return node.whenRunning(() -> {
            long start = clock.currentTimeInNanos();
            // Check the privilege first ...
            CompositeAction check = env.getSecurity().check(token);
            batch.forEach(patch -> {
                String dbId = patch.target().databaseId().asString();
                if (patch.isReadRequest()) check.canRead(dbId);
                else if (patch.isDeletion()) check.canWrite(dbId);
                else if (patch.isEmpty()) {}
                else check.canWrite(dbId);
            });
            String username = check.submit();
            if (username == null) {
                throw new DebeziumAuthorizationException("Unable to submit batch against database(s) " + check);
            }
            int count = batch.patchCount();
            Map<String, Entity> reads = new ConcurrentHashMap<>();
            Map<String, EntityChange> changes = new ConcurrentHashMap<>();
            Set<String> destroys = new HashSet<>();
            logger.debug("Attempting to submit batch with {} patches against database(s): {}", count, check);
            partialResponses.submit(count, requestId -> {
                Document request = batch.asDocument();
                Message.addHeaders(request, requestId.getClientId(), requestId.getRequestNumber(), username);
                if (!node.send(Topic.ENTITY_BATCHES, requestId.asString(), request)) {
                    throw new DebeziumClientException("Unable to send batch with " + count + " patches against database(s) " + check);
                }
            }).onEachResponse(timeout, unit, response -> {
                EntityId id = Message.getEntityId(response);
                Document representation = Message.getAfterOrBefore(response);
                Patch<EntityId> patch = Patch.forEntity(response);
                if ( patch == null ) {
                    // We read the entity ...
                    reads.put(id.asString(), new DbzEntity(id, representation));
                } else if ( patch.isDeletion() ) {
                    destroys.add(id.asString());
                } else {
                    Entity entity = new DbzEntity(id, representation);
                    ChangeStatus status = changeStatus(Message.getStatus(response));
                    Collection<String> failureReasons = Message.getFailureReasons(response);
                    changes.put(id.asString(), new DbzEntityChange(patch, entity, status, failureReasons));
                }
            }).onTimeout(() -> {
                throw new DebeziumTimeoutException("The request timed out while submitting batch with " + count
                        + " patches against database(s): " + check);
            });
            logUsage(token, null, duration(start), "submitBatch", "parts", count);
            return new DbzBatchResult(reads, changes, destroys);
        }).orElseThrow(DebeziumClientException::new);
    }

    private static ChangeStatus changeStatus(Message.Status messageStatus) {
        switch (messageStatus) {
            case SUCCESS:
                return ChangeStatus.OK;
            case PATCH_FAILED:
                return ChangeStatus.PATCH_FAILED;
            case DOES_NOT_EXIST:
                return ChangeStatus.DOES_NOT_EXIST;
        }
        throw new IllegalStateException("Unknown status: " + messageStatus);
    }

    @Override
    public void shutdown(long timeout, TimeUnit unit) {
        // Shutdown the cluster node, which shuts down all services and the service manager ...
        try {
            node.shutdown();
        } finally {
            env.shutdown(timeout, unit);
        }
    }

}
