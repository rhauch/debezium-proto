/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.debezium.annotation.Immutable;
import org.debezium.model.Entity;
import org.debezium.model.EntityChange;

/**
 * A representation of a {@link BatchResult}.
 * @author Randall Hauch
 */
@Immutable
final class DbzBatchResult implements BatchResult {

    private final Map<String,Entity> reads;
    private final Map<String,EntityChange> changes;
    private final Set<String> removals;
    
    public DbzBatchResult( Map<String,Entity> reads, Map<String,EntityChange> changes, Set<String> removals ) {
        this.reads = Collections.unmodifiableMap(reads);
        this.changes = Collections.unmodifiableMap(changes);
        this.removals = Collections.unmodifiableSet(removals);
    }

    @Override
    public Map<String, Entity> reads() {
        return reads;
    }

    @Override
    public Map<String, EntityChange> changes() {
        return changes;
    }

    @Override
    public Set<String> removals() {
        return removals;
    }

}
