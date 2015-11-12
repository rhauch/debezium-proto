/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.model;

import java.util.Collection;
import java.util.Collections;

import org.debezium.annotation.Immutable;
import org.debezium.message.Patch;

/**
 * The result of a request to change a single entity type.
 * 
 * @author Randall Hauch
 */
@Immutable
public final class EntityTypeChange {

    private static final Collection<String> NO_FAILURE_REASONS = Collections.emptyList();

    /**
     * Create a schema change with the given information.
     * 
     * @param patch the patch; may not be null
     * @param collection the entity type collection; may be null if the collection didn't exist when the patch was applied
     * @param status the status; may not be null
     * @param failureReasons the failure reasons; may be null
     * @return the schema change; never null
     */
    public static EntityTypeChange with(Patch<EntityType> patch, EntityCollection collection, ChangeStatus status,
                                        Collection<String> failureReasons) {
        if (patch == null) throw new IllegalArgumentException("The 'patch' parameter may not be null");
        if (status == null) throw new IllegalArgumentException("The 'status' parameter may not be null");
        return new EntityTypeChange(patch, collection, status, failureReasons);
    }

    private final Patch<EntityType> patch;
    private final EntityCollection collection;
    private final ChangeStatus status;
    private final Collection<String> failureReasons;

    private EntityTypeChange(Patch<EntityType> patch, EntityCollection collection, ChangeStatus status, Collection<String> failureReasons) {
        this.patch = patch;
        this.collection = collection;
        this.status = status;
        this.failureReasons = failureReasons != null ? failureReasons : NO_FAILURE_REASONS;
    }

    /**
     * Get the patch that describes the change.
     * 
     * @return the patch; never null
     */
    public Patch<EntityType> patch() {
        return patch;
    }

    /**
     * Get current representation of the entity type definition after the patch was applied. The representation can be used in the
     * case of a {@link ChangeStatus#PATCH_FAILED failed patch} to rebuild a new patch.
     * 
     * @return the target, or null if the target didn't exist when the patch was applied.
     */
    public EntityCollection entityType() {
        return collection;
    }

    /**
     * Get the identifier of the entity.
     * 
     * @return the identifier; never null
     */
    public String id() {
        return patch.target().toString();
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
     * @return the failure reasons; never null, but empty if the operation {@link #succeeded() succeeded}.
     */
    public Collection<String> failureReasons() {
        return failureReasons;
    }
}
