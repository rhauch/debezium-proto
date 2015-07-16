/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A set of utilities for more easily creating various kinds of collections.
 */
public class Collect {

    public static <T> Set<T> unmodifiableSet( @SuppressWarnings( "unchecked" ) T... values ) {
        return unmodifiableSet(arrayListOf(values));
    }

    public static <T> Set<T> unmodifiableSet( Collection<T> values ) {
        return Collections.unmodifiableSet(new HashSet<T>(values));
    }

    public static <T> Set<T> unmodifiableSet( Set<T> values ) {
        return Collections.unmodifiableSet(values);
    }
    
    public static <T> List<T> arrayListOf( T[] values ) {
        List<T> result = new ArrayList<>();
        for ( T value : values ) {
            if ( value != null ) result.add(value);
        }
        return result;
    }
    
    public static <T> List<T> arrayListOf( T first, @SuppressWarnings("unchecked") T...additional ) {
        List<T> result = new ArrayList<>();
        result.add(first);
        for ( T another : additional ) {
            if ( another != null ) result.add(another);
        }
        return result;
    }
    
    public static <T> List<T> arrayListOf( Iterable<T> values ) {
        List<T> result = new ArrayList<>();
        values.forEach((value)->result.add(value));
        return result;
    }
    
    
    public static <K,V> Map<K,V> mapOf( K key, V value ) {
        return Collections.singletonMap(key, value);
    }

    public static <K,V> Map<K,V> hashMapOf( K key, V value ) {
        Map<K,V> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    public static <K,V> Map<K,V> hashMapOf( K key1, V value1, K key2, V value2 ) {
        Map<K,V> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }

    public static <K,V> Map<K,V> hashMapOf( K key1, V value1, K key2, V value2, K key3, V value3 ) {
        Map<K,V> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        map.put(key3, value3);
        return map;
    }

    public static <K,V> Map<K,V> hashMapOf( K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4 ) {
        Map<K,V> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        map.put(key3, value3);
        map.put(key4, value4);
        return map;
    }

    private Collect() {
    }
}
