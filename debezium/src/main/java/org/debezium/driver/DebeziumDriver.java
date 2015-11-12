/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.concurrent.TimeUnit;

import org.debezium.Configuration;
import org.debezium.message.Patch;
import org.debezium.message.Patch.Editor;
import org.debezium.model.Entity;
import org.debezium.model.EntityChange;
import org.debezium.model.EntityId;
import org.debezium.model.EntityType;
import org.debezium.model.EntityTypeChange;
import org.debezium.model.Schema;

/**
 * The public interface for the Debezium Driver, a client that connects directly to the Kafka brokers to produce requests
 * and consume responses.
 * <h2>Schemas</h2>
 * <p>
 * In Debezium, schemas provide information about the entities stored within the database's collections. Debezium
 * automatically derives the schemas by examining the structure of all entities stored within each collection. As new entities
 * are added or existing entities are changed or removed, Debezium automatically updates the derived schema to maintain an
 * accurate description of all entities.
 * <p>
 * Debezium does not use these schemas to validate entity changes before they are applied, so it is up to you to ensure that
 * the entities of a particular type adhere to your preferred structure. This gives you and your applications a lot of power
 * to evolve your entities over time by adding, changing, and removing fields as needed. However, it also means that some
 * fields defined by an entity type may not be used in all entities, so Debezium includes in its schemas usage metrics for
 * each field that capture what percentage of the entities contain the field.
 * <p>
 * The automatically derived schemas are completely accurate and kept up-to-date as entities are added, changed, and
 * removed. But, as you evolve your entity types these derived descriptions may not reflect your <em>semantic intent</em>.
 * For this reason, Debezium lets you manually modify the schema using patches created with this builder. These patches
 * will accumulate and override the automatically-derived schemas, allowing you to make the exposed schemas reflect your
 * intended structure (perhaps to use dynamically within applications to either validate or inform the structure of submitted
 * entities).
 * <p>
 * You can also remove the manually-modified fields from the schemas, although the schema may still include a
 * field definition for any field still used within persisted entities. In other words, manually remove all fields from the
 * schema simply removes all manual schema changes, and the schema reverts to the automatically derived schema.
 * 
 * @author Randall Hauch
 */
public interface DebeziumDriver {

    /**
     * Create a fluent API to programmatically build up the configuration for a driver.
     * 
     * @param configuration the configuration to be used by the driver; may not be null
     * @param env the environment; may not be null
     * @return the new builder; never null
     */
    public static DebeziumDriver create(Configuration configuration, Environment env) {
        return new DbzDriver(configuration, env).start();
    }

    /**
     * Authenticate the named user for the given database.
     * 
     * @param username the username
     * @param device the device identifier or token
     * @param appVersion the version of the application
     * @param databaseIds the identifier of the database(s) for which the user is to be authenticated
     * @return the session token for the user; never null
     * @throws DebeziumAuthorizationException if the user could not be authenticated or authorized for the database(s)
     */
    public SessionToken connect(String username, String device, String appVersion, String... databaseIds);

    /**
     * Provision a new database with the given name, blocking at most the default timeout.
     * 
     * @param adminToken a valid session token for an administrative user; may not be null
     * @param databaseId the identifier for the new database; may not be null
     * @param timeout the maximum amount of time to wait for responses
     * @param unit the unit of time for the timeout
     * @throws DebeziumAuthorizationException if the user was not authorized to perform this operation
     * @throws DebeziumTimeoutException if the operation timed out
     * @throws DebeziumProvisioningException if there was an error provisioning a database with the given username, for
     *             example if the database already exists
     */
    public void provision(SessionToken adminToken, String databaseId, long timeout, TimeUnit unit);

    /**
     * Read the current schema for the specified database.
     * 
     * @param token a valid session token for the user; may not be null
     * @param databaseId the name of the database; may not be null
     * @param timeout the maximum amount of time to wait for responses
     * @param unit the unit of time for the timeout
     * @return the schema; never null
     * @throws DebeziumAuthorizationException if the user was not authorized to perform this operation
     * @throws DebeziumTimeoutException if the operation timed out
     */
    public Schema readSchema(SessionToken token, String databaseId, long timeout, TimeUnit unit);

    /**
     * Request to apply the given patch to an entity type within the database's schema.
     * 
     * @param token a valid session token for the user; may not be null
     * @param patch the patch; may not be null
     * @param timeout the amount of time to wait for the response
     * @param unit the unit of time for the timeout
     * @return the result of the change request; never null
     * @throws DebeziumAuthorizationException if the user was not authorized to perform this operation
     * @throws DebeziumTimeoutException if the operation timed out
     */
    public EntityTypeChange changeEntityType(SessionToken token, Patch<EntityType> patch, long timeout, TimeUnit unit);

    /**
     * Read one entity from the database and return its representation, including whether or not the entity
     * {@link Entity#exists() exists}.
     * 
     * @param token a valid session token for the user; may not be null
     * @param entityId the entity's unique identifier; may not be null
     * @param timeout the amount of time to wait for the response
     * @param unit the unit of time for the timeout
     * @return a representation of the entity; never null
     * @throws DebeziumAuthorizationException if the user was not authorized to perform this operation
     * @throws DebeziumTimeoutException if the operation timed out
     */
    public Entity readEntity(SessionToken token, EntityId entityId, long timeout, TimeUnit unit);

    /**
     * Request to apply the given patch to an entity.
     * 
     * @param token a valid session token for the user; may not be null
     * @param patch the patch; may not be null
     * @param timeout the amount of time to wait for the response
     * @param unit the unit of time for the timeout
     * @return the result of the change request; never null
     * @throws DebeziumAuthorizationException if the user was not authorized to perform this operation
     * @throws DebeziumTimeoutException if the operation timed out
     */
    public EntityChange changeEntity(SessionToken token, Patch<EntityId> patch, long timeout, TimeUnit unit);

    /**
     * Destroy one entity from the database.
     * 
     * @param token a valid session token for the user; may not be null
     * @param entityId the entity's unique identifier within this database; may not be null
     * @param timeout the amount of time to wait for the response
     * @param unit the unit of time for the timeout
     * @return {@code true} if the entity existed and was destroyed, or {@code false} if it did not exist
     * @throws DebeziumAuthorizationException if the user was not authorized to perform this operation
     * @throws DebeziumTimeoutException if the operation timed out
     */
    public boolean destroyEntity(SessionToken token, EntityId entityId, long timeout, TimeUnit unit);

    /**
     * Begin a batch operation. Use the resulting {@link BatchBuilder} object to assemble the requests, and then
     * {@link BatchBuilder#submit(SessionToken, long, TimeUnit)} the batch.
     * 
     * @return the builder of the batch request; never null
     */
    public BatchBuilder batch();

    /**
     * Shutdown this client and release all resources.
     * 
     * @param timeout the maximum time that this method should block before returning; must be positive
     * @param unit the time unit for {@code timeout}; may not be null
     * @throws DebeziumTimeoutException if the operation timed out
     */
    public void shutdown(long timeout, TimeUnit unit);

    /**
     * Get the immutable configuration used by this instance.
     * 
     * @return the configuration; never null
     */
    public Configuration getConfiguration();

    /**
     * A builder of a batch request, used to record the operations for a single database that are to be
     * {@link #submit(SessionToken, long, TimeUnit) submitted} to the server using a single network request.
     * 
     * @author Randall Hauch
     */
    public static interface BatchBuilder {
        /**
         * Add a request to this batch to read one entity from the database and return its representation, including whether or
         * not the entity {@link Entity#exists() exists}.
         * 
         * @param entityId the entity's unique identifier within this database; may not be null
         * @return this builder instance for chaining together methods; never null
         */
        public BatchBuilder readEntity(EntityId entityId);

        /**
         * Add a request to this batch to apply the given patch to an entity.
         * 
         * @param patch the patch; may not be null
         * @return this builder instance for chaining together methods; never null
         */
        public BatchBuilder changeEntity(Patch<EntityId> patch);

        /**
         * Add a request to this batch to destroy one entity from the database.
         * 
         * @param entityId the entity's unique identifier within this database; may not be null
         * @return this builder instance for chaining together methods; never null
         */
        public BatchBuilder destroyEntity(EntityId entityId);

        /**
         * Add a request that changes the entity with the given ID, returning a {@link Editor patch editor} that can be used
         * to record the operations. Call {@link Editor#end()} on the patch editor to obtain this batch builder.
         * 
         * @param entityId the entity's unique identifier within this database; may not be null
         * @return the patch editor, which when {@link Editor#end()} is called returns this builder
         */
        public Patch.Editor<BatchBuilder> changeEntity(EntityId entityId);

        /**
         * Add a request that creates a new entity of the given type and with a generated ID, returning a {@link Editor patch
         * editor} that can be used to record the operations. Call {@link Editor#end()} on the patch editor to obtain this
         * batch builder.
         * 
         * @param entityType the type of entity to be created; may not be null
         * @return the patch editor, which when {@link Editor#end()} is called returns this builder
         */
        public Patch.Editor<BatchBuilder> createEntity(EntityType entityType);

        /**
         * Submit the recorded operations to the server as a single batched request, wait for the response, and return the
         * results.
         * 
         * @param token a valid session token for the user; may not be null
         * @param timeout the amount of time to wait for the response
         * @param unit the unit of time for the timeout
         * @return the results of the batched operations; never null
         * @throws DebeziumAuthorizationException if the user was not authorized to perform this operation
         * @throws DebeziumTimeoutException if the operation timed out
         */
        public BatchResult submit(SessionToken token, long timeout, TimeUnit unit);
    }

}
