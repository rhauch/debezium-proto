/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.util;

import java.util.Iterator;
import java.util.stream.IntStream;

/**
 * @author Randall Hauch
 *
 */
public class Sequences {
    
    public static IntStream times( int number ) {
        return IntStream.range(0, number);
    }
    
    public static Iterable<Integer> infiniteIntegers() {
        return infiniteIntegers(0);
    }
        
    public static Iterable<Integer> infiniteIntegers( int startingAt ) {
        return Iterators.around(new Iterator<Integer>() {
            private int counter = startingAt;
            @Override
            public boolean hasNext() {
                return true;
            }
            @Override
            public Integer next() {
                return Integer.valueOf(counter++);
            }
        });
    }

}
