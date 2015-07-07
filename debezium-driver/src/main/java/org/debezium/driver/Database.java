/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.io.Closeable;
import java.util.Collections;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.Entity;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.Identified;
import org.debezium.core.component.Identifier;
import org.debezium.core.component.Schema;
import org.debezium.core.message.Batch;
import org.debezium.core.message.Batch.Builder;
import org.debezium.core.message.Patch;
import org.debezium.core.message.Patch.Action;

/**
 * Representation of a Debezium database.
 * 
 * @author Randall Hauch
 */
public interface Database extends Closeable {

    /**
     * Get the identifier of this database.
     * 
     * @return the database ID; never null
     */
    public DatabaseId databaseId();

    /**
     * Non-blocking method that read the database's schema.
     * 
     * @param handler the function to be asynchronously called by this method when it has obtained the schema; may not be null
     * @return a completion object that acts as a future and that the caller can use to block until the handler has completed
     *         processing its result; never null
     */
    public Completion readSchema(OutcomeHandler<Schema> handler);

    /**
     * Non-blocking method that read one or more entities from the database.
     * 
     * @param entityIds the identifiers of the entities to be read; may not be null, but may be empty
     * @param handler the function to be asynchronously called by this method when it has obtained the first entity; may not be
     *            null
     * @return a completion object that acts as a future and that the caller can use to block until the handler has completed
     *         processing its result; never null
     */
    public Completion readEntities(Iterable<EntityId> entityIds, OutcomeHandler<Stream<Entity>> handler);

    /**
     * Non-blocking method that reads one entity from the database.
     * 
     * @param entityId the identifier of the entity to be read; may not be null
     * @param handler the function to be asynchronously called by this method when it has obtained the entity; may not be null
     * @return a completion object that acts as a future and that the caller can use to block until the handler has completed
     *         processing its result; never null
     */
    default public Completion readEntity(EntityId entityId, OutcomeHandler<Stream<Entity>> handler) {
        return readEntities(Collections.singleton(entityId), handler);
    }

    /**
     * Non-blocking method that submits a batch of changes to be applied to one or more entities. The {@link Batch batch} can
     * contain multiple {@link Patch patches}, each of which can perform multiple operations against a single entity.
     * For example, a patch might read the whole entity, or it might add, set, replace, move, copy, or require fields.
     * <p>
     * Batches are created using the {@link Batch#entities()} method, which returns a {@link Builder} that can add multiple
     * {@link Patch patches}.
     * 
     * @param batch the batch of changes; may not be null
     * @param handler the function to be asynchronously called by this method when it has obtained the results from the first
     *            patch
     *            in the batch; may not be null
     * @return a completion object that acts as a future and that the caller can use to block until the handler has completed
     *         processing its result; never null
     */
    public Completion changeEntities(Batch<EntityId> batch, OutcomeHandler<Stream<Change<EntityId, Entity>>> handler);

    /**
     * Determine if this database is connected to the Debezium backend.
     * 
     * @return true if the database is connected, or false otherwise
     */
    public boolean isConnected();

    /**
     * Close the connection to the backend database. This database object cannot be used after it is closed.
     */
    @Override
    public void close();

    /**
     * The result of an asynchronous method that can be used to {@link #await() wait} while the method completes. This is a
     * simpler
     * form of the standard {@link Future}.
     * 
     * @author Randall Hauch
     */
    public static interface Completion {
        /**
         * Determine if this is already complete.
         * 
         * @return true if complete, or false if it is not yet completed and {@link #await()} or {@link #await(long, TimeUnit)}
         *         can
         *         be called to wait until complete
         */
        public boolean isComplete();

        /**
         * Waits if necessary for the operation to complete, and then
         * retrieves its result.
         *
         * @throws InterruptedException if the current thread was interrupted
         *             while waiting
         */
        public void await() throws InterruptedException;

        /**
         * Waits if necessary for at most the given time for the operation
         * to complete, and then retrieves its result, if available.
         *
         * @param timeout the maximum time to wait
         * @param unit the time unit of the timeout argument
         * @throws InterruptedException if the current thread was interrupted
         *             while waiting
         * @throws TimeoutException if the wait timed out
         */
        public void await(long timeout, TimeUnit unit)
                throws InterruptedException, TimeoutException;
    }

    /**
     * A function supplied by the caller of a {@link Database} method to receive and handle the outcome of the operation.
     * 
     * @author Randall Hauch
     * @param <T> the type of the operation's result
     */
    @FunctionalInterface
    public static interface OutcomeHandler<T> {
        /**
         * Receive the outcome of the operation, called by the {@link Database} when the invoked method has some output
         * to report back to the caller.
         * 
         * @param outcome the outcome; never null
         */
        public void handle(Outcome<T> outcome);
    }

    /**
     * The outcome of an operation.
     * 
     * @author Randall Hauch
     * @param <T> the type of the operation's result
     */
    public static interface Outcome<T> {
        public static enum Status {
            /** Status that signifies the operation was successful. */
            OK,
            /** Status that signifies the operation was timed out before it completed. */
            TIMEOUT,
            /** Status that signifies the client was stopped before the operation completed. */
            CLIENT_STOPPED,
            /**
             * Status that signifies there was a communication error, and that it is unknown whether the operation was
             * was actually submitted or, if it was, then whether it completed.
             */
            COMMUNICATION_ERROR
        }

        /**
         * Determine whether the operation did not succeed. This is a convenience method that is equivalent to
         * {@code !succeeded()} or {@code status() != Status.OK}.
         * 
         * @return true if the operation failed, or false otherwise
         * @see #succeeded()
         */
        default public boolean failed() {
            return !succeeded();
        }

        /**
         * Determine whether the operation succeeded. This is a convenience method that is equivalent to
         * {@code status() == Status.OK}.
         * 
         * @return true if the operation succeeded, or false otherwise
         * @see #succeeded()
         */
        default public boolean succeeded() {
            return status() == Status.OK;
        }

        /**
         * Get the status of the operation.
         * 
         * @return the outcome status; never null
         */
        public Status status();

        /**
         * Get the reason why the operation failed, if it was not successful.
         * 
         * @return the failure reason, or null if the operation {@link #succeeded() succeeded}.
         */
        public String failureReason();

        /**
         * Get the result of the successful operation.
         * 
         * @return the operation result, or null if the operation {@link #failed() failed}.
         */
        public T result();
    }

    /**
     * The status of a requested change operation.
     * 
     * @author Randall Hauch
     */
    public static enum ChangeStatus {
        /** The change was successfully applied. */
        OK,
        /** The requested change could not be made because the target of the change no longer exists. */
        DOES_NOT_EXIST,
        /**
         * The requested change could not be made because at least one {@link Action#REQUIRE requirement} within the patch
         * could not be satisfied.
         */
        PATCH_FAILED;
    }

    /**
     * The result of a request to change a single target.
     * 
     * @author Randall Hauch
     * @param <IdType> the type of the target's identifier
     * @param <TargetType> the type of the target
     */
    public static interface Change<IdType extends Identifier, TargetType extends Identified<IdType>> {

        /**
         * Get the patch that describes the change.
         * 
         * @return the patch; never null
         */
        public Patch<IdType> patch();

        /**
         * Get current representation of the target at the time the patch was applied. The target can be used in the case of a
         * {@link ChangeStatus#PATCH_FAILED failed patch} to rebuild a new patch.
         * 
         * @return the target, or null if the target didn't exist when the patch was applied.
         */
        public TargetType target();

        /**
         * Get the identifier of the target.
         * 
         * @return the identifier; never null
         */
        public IdType id();

        /**
         * Determine whether the change did not succeed. This is a convenience method that is equivalent to {@code !succeeded()}
         * or {@code status() != ChangeStatus.OK}.
         * 
         * @return true if the operation failed, or false otherwise
         * @see #succeeded()
         */
        default public boolean failed() {
            return !succeeded();
        }

        /**
         * Determine whether the operation succeeded. This is a convenience method that is equivalent to
         * {@code status() == ChangeStatus.OK}.
         * 
         * @return true if the operation succeeded, or false otherwise
         * @see #succeeded()
         */
        default public boolean succeeded() {
            return status() == ChangeStatus.OK;
        }

        /**
         * Get the status of the change operation.
         * 
         * @return the outcome status; never null
         */
        public ChangeStatus status();

        /**
         * Get the reason(s) why the change failed, if it was not successful.
         * 
         * @return the failure reasons, or null if the operation {@link #succeeded() succeeded}.
         */
        public Stream<String> failureReasons();
    }
}
