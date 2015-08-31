/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.crdt;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class StateBasedGCounterTest {

    @Test
    public void shouldCreateGCounterWithZeroInitialValues() {
        GCounter simple = CRDT.newGCounter();
        assertThat(simple.get()).isEqualTo(0);
        assertThat(simple.getIncrement()).isEqualTo(0);
    }

    @Test
    public void shouldCreateGCounterWithSuppliedValue() {
        GCounter simple = CRDT.newGCounter(41);
        assertThat(simple.get()).isEqualTo(41);
        assertThat(simple.getIncrement()).isEqualTo(41);
    }

    @Test
    public void shouldCreateGCounterThenIncrementMultipleTimes() {
        GCounter simple = CRDT.newGCounter();
        for ( int i=0; i!=10; ++i ) {
            simple.increment();
        }
        assertThat(simple.get()).isEqualTo(10);
        assertThat(simple.getIncrement()).isEqualTo(10);
    }

    @Test
    public void shouldCreateGCounterThenIncrementAndGetMultipleTimes() {
        GCounter simple = CRDT.newGCounter();
        for ( int i=0; i!=10; ++i ) {
            long value = simple.incrementAndGet();
            assertThat(simple.get()).isEqualTo(value);
            assertThat(simple.getIncrement()).isEqualTo(value);
        }
        assertThat(simple.get()).isEqualTo(10);
        assertThat(simple.getIncrement()).isEqualTo(10);
    }

    @Test
    public void shouldCreateGCounterThenGetAndIncrementMultipleTimes() {
        GCounter simple = CRDT.newGCounter();
        for ( int i=0; i!=10; ++i ) {
            long value = simple.getAndIncrement();
            assertThat(simple.get()).isEqualTo(value+1);
            assertThat(simple.getIncrement()).isEqualTo(value+1);
        }
        assertThat(simple.get()).isEqualTo(10);
        assertThat(simple.getIncrement()).isEqualTo(10);
    }

    @Test
    public void shouldCreateTwoGCountersAndMergeThem() {
        GCounter c1 = CRDT.newGCounter(41);
        GCounter c2 = CRDT.newGCounter(22).merge(c1);
        assertThat(c2.get()).isEqualTo(41+22);
        assertThat(c2.getIncrement()).isEqualTo(41+22);
    }
}
