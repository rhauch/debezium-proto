/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A set of utilities for more easily creating various kinds of collections.
 */
public class Collect {

    public static <T> Set<T> unmodifiableSet( @SuppressWarnings( "unchecked" ) T... values ) {
        return unmodifiableSet(Arrays.asList(values));
    }

    public static <T> Set<T> unmodifiableSet( Collection<T> values ) {
        return Collections.unmodifiableSet(new HashSet<T>(values));
    }

    public static <T> Set<T> unmodifiableSet( Set<T> values ) {
        return Collections.unmodifiableSet(values);
    }

    private Collect() {
    }
}
