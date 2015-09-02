/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.model;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;

import org.debezium.annotation.Immutable;
import org.debezium.message.Patch;
import org.debezium.message.Patch.Action;


/**
 * The result of a request to change a single entity.
 * 
 * @author Randall Hauch
 */
@Immutable
public final class EntityChange {

    private static final Collection<String> NO_FAILURE_REASONS = Collections.emptyList();

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
     * Create an entity change with the given information.
     * @param patch the patch; may not be null
     * @param entity the entity; may be null if the entity didn't exist when the patch was applied
     * @param status the status; may not be null
     * @param failureReasons the failure reasons; may be null
     * @return the entity; never null
     */
    public static EntityChange with(Patch<EntityId> patch, Entity entity, ChangeStatus status, Collection<String> failureReasons) {
        if (patch == null) throw new IllegalArgumentException("The 'patch' parameter may not be null");
        if (status == null) throw new IllegalArgumentException("The 'status' parameter may not be null");
        return new EntityChange(patch,entity,status,failureReasons);
    }
    
    private final Patch<EntityId> patch;
    private final Entity entity;
    private final ChangeStatus status;
    private final Collection<String> failureReasons;
    
    private EntityChange( Patch<EntityId> patch, Entity entity, ChangeStatus status, Collection<String> failureReasons ) {
        this.patch = patch;
        this.entity = entity;
        this.status = status;
        this.failureReasons = failureReasons != null ? failureReasons : NO_FAILURE_REASONS;
    }

    /**
     * Get the patch that describes the change.
     * 
     * @return the patch; never null
     */
    public Patch<EntityId> patch() {
        return patch;
    }

    /**
     * Get current representation of the target at the time the patch was applied. The target can be used in the case of a
     * {@link ChangeStatus#PATCH_FAILED failed patch} to rebuild a new patch.
     * 
     * @return the target, or null if the target didn't exist when the patch was applied.
     */
    public Entity entity() {
        return entity;
    }

    /**
     * Get the identifier of the entity.
     * 
     * @return the identifier; never null
     */
    public String id() {
        return entity().id().toString();
    }

    /**
     * Determine whether the change did not succeed. This is a convenience method that is equivalent to {@code !succeeded()}
     * or {@code status() != ChangeStatus.OK}.
     * 
     * @return true if the operation failed, or false otherwise
     * @see #succeeded()
     */
    public boolean failed() {
        return !succeeded();
    }

    /**
     * Determine whether the operation succeeded. This is a convenience method that is equivalent to
     * {@code status() == ChangeStatus.OK}.
     * 
     * @return true if the operation succeeded, or false otherwise
     * @see #succeeded()
     */
    public boolean succeeded() {
        return status() == ChangeStatus.OK;
    }

    /**
     * Get the status of the change operation.
     * 
     * @return the outcome status; never null
     */
    public ChangeStatus status() {
        return status;
    }

    /**
     * Get the reason(s) why the change failed, if it was not successful.
     * 
     * @return the failure reasons, or null if the operation {@link #succeeded() succeeded}.
     */
    public Stream<String> failureReasons() {
        return failureReasons.stream();
    }
}
