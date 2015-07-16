/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.stream.Stream;

import org.debezium.core.annotation.Immutable;
import org.debezium.core.component.EntityId;
import org.debezium.core.message.Patch;
import org.debezium.core.message.Patch.Action;

/**
 * The result of a request to change a single entity.
 * 
 * @author Randall Hauch
 */
@Immutable
public interface EntityChange {

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
     * Get the patch that describes the change.
     * 
     * @return the patch; never null
     */
    public Patch<EntityId> patch();

    /**
     * Get current representation of the target at the time the patch was applied. The target can be used in the case of a
     * {@link ChangeStatus#PATCH_FAILED failed patch} to rebuild a new patch.
     * 
     * @return the target, or null if the target didn't exist when the patch was applied.
     */
    public Entity entity();

    /**
     * Get the identifier of the entity.
     * 
     * @return the identifier; never null
     */
    default public String id() {
        return entity().id().toString();
    }

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
