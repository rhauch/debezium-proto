/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;

import org.debezium.core.annotation.Immutable;
import org.debezium.core.component.EntityId;
import org.debezium.core.message.Patch;

/**
 * A representation of an {@link EntityChange} that may or may not have succeeded.
 * @author Randall Hauch
 */
@Immutable
final class DbzEntityChange implements EntityChange {
    
    private static final Collection<String> NO_FAILURE_REASONS = Collections.emptyList();

    private final Patch<EntityId> patch;
    private final Entity entity;
    private final ChangeStatus status;
    private final Collection<String> failureReasons;
    
    public DbzEntityChange( Patch<EntityId> patch, Entity entity, ChangeStatus status, Collection<String> failureReasons ) {
        this.patch = patch;
        this.entity = entity;
        this.status = status;
        this.failureReasons = failureReasons != null ? failureReasons : NO_FAILURE_REASONS;
    }

    @Override
    public Patch<EntityId> patch() {
        return patch;
    }

    @Override
    public Entity entity() {
        return entity;
    }

    @Override
    public String id() {
        return patch.target().asString();
    }

    @Override
    public ChangeStatus status() {
        return status;
    }

    @Override
    public Stream<String> failureReasons() {
        return failureReasons.stream();
    }

}
