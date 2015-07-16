/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.debezium.core.annotation.Immutable;

/**
 * The results of a {@link Debezium#batch() operation}.
 * @author Randall Hauch
 */
@Immutable
public interface BatchResult {

    /**
     * Determine if these results are empty.
     * 
     * @return {@code true} if there are no {@link #hasReads() reads}, no {@link #hasChanges() changes}, and no
     *         {@link #hasRemovals() destroys}, or {@code false} otherwise
     */
    default public boolean isEmpty() {
        return !hasReads() && !hasChanges() && !hasRemovals();
    }

    /**
     * Determine if these results contain at least one {@link #reads() read result}.
     * 
     * @return {@code true} if there is at least one read result, or {@code false} if there are none
     */
    default public boolean hasReads() {
        return !reads().isEmpty();
    }

    /**
     * Determine if these results contain at least one {@link #changes() change result}.
     * 
     * @return {@code true} if there is at least one change result, or {@code false} if there are none
     */
    default public boolean hasChanges() {
        return !changes().isEmpty();
    }

    /**
     * Determine if these results contain at least one {@link #removals() removal result}.
     * 
     * @return {@code true} if there is at least one destroy result, or {@code false} if there are none
     */
    default public boolean hasRemovals() {
        return !removals().isEmpty();
    }

    /**
     * Get the map of {@link Entity} read results keyed by entity identifier.
     * @return the map of read results by identifier; never null but possibly empty
     */
    public Map<String, Entity> reads();

    /**
     * Get the map of {@link EntityChange} results keyed by entity identifier.
     * @return the map of change results by identifier; never null but possibly empty
     */
    public Map<String, EntityChange> changes();

    /**
     * Get the set of entity identifiers that were successfully removed.
     * @return the identifiers of the removed entities; never null but possibly empty
     */
    public Set<String> removals();

    /**
     * Get the stream of {@link Entity} read results.
     * @return the stream of read results; never null but possibly empty
     */
    default public Stream<Entity> readStream() {
        return reads().values().stream();
    }

    /**
     * Get the stream of {@link EntityChange} results keyed by entity identifier.
     * @return the stream of change results; never null but possibly empty
     */
    default public Stream<EntityChange> changeStream() {
        return changes().values().stream();
    }

    /**
     * Get the stream of entity identifiers that were successfully removed.
     * @return the identifiers of the removed entities; never null but possibly empty
     */
    default public Stream<String> removalStream() {
        return removals().stream();
    }

}
