/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.crdt;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class StateBasedPNCounterTest {

    @Test
    public void shouldCreatePNCounterWithZeroInitialValues() {
        PNCounter simple = CRDT.newPNCounter();
        assertThat(simple.get()).isEqualTo(0);
        assertThat(simple.getIncrement()).isEqualTo(0);
        assertThat(simple.getDecrement()).isEqualTo(0);
    }

    @Test
    public void shouldCreatePNCounterWithSuppliedValue() {
        PNCounter simple = CRDT.newPNCounter(100,41);
        assertThat(simple.get()).isEqualTo(100-41);
        assertThat(simple.getIncrement()).isEqualTo(100);
        assertThat(simple.getDecrement()).isEqualTo(41);
    }

    @Test
    public void shouldCreatePNCounterThenIncrementMultipleTimes() {
        PNCounter simple = CRDT.newPNCounter();
        for ( int i=0; i!=10; ++i ) {
            simple.increment();
        }
        assertThat(simple.get()).isEqualTo(10);
        assertThat(simple.getIncrement()).isEqualTo(10);
        assertThat(simple.getDecrement()).isEqualTo(0);
    }

    @Test
    public void shouldCreatePNCounterThenDecrementMultipleTimes() {
        PNCounter simple = CRDT.newPNCounter();
        for ( int i=0; i!=10; ++i ) {
            simple.decrement();
        }
        assertThat(simple.get()).isEqualTo(-10);
        assertThat(simple.getIncrement()).isEqualTo(0);
        assertThat(simple.getDecrement()).isEqualTo(10);
    }

    @Test
    public void shouldCreatePNCounterThenIncrementAndDecrementMultipleTimes() {
        PNCounter simple = CRDT.newPNCounter();
        for ( int i=0; i!=10; ++i ) {
            simple.increment();
        }
        for ( int i=0; i!=5; ++i ) {
            simple.decrement();
        }
        assertThat(simple.get()).isEqualTo(5);
        assertThat(simple.getIncrement()).isEqualTo(10);
        assertThat(simple.getDecrement()).isEqualTo(5);
    }

    @Test
    public void shouldCreatePNCounterThenIncrementAndGetMultipleTimes() {
        PNCounter simple = CRDT.newPNCounter();
        for ( int i=0; i!=10; ++i ) {
            long value = simple.incrementAndGet();
            assertThat(simple.get()).isEqualTo(value);
            assertThat(simple.getIncrement()).isEqualTo(value);
            assertThat(simple.getDecrement()).isEqualTo(0);
        }
        assertThat(simple.get()).isEqualTo(10);
        assertThat(simple.getIncrement()).isEqualTo(10);
        assertThat(simple.getDecrement()).isEqualTo(0);
    }

    @Test
    public void shouldCreatePNCounterThenDecrementAndGetMultipleTimes() {
        PNCounter simple = CRDT.newPNCounter();
        for ( int i=0; i!=10; ++i ) {
            long value = simple.decrementAndGet();
            assertThat(simple.get()).isEqualTo(value);
            assertThat(simple.getIncrement()).isEqualTo(0);
            assertThat(simple.getDecrement()).isEqualTo(-1*value);
        }
        assertThat(simple.get()).isEqualTo(-10);
        assertThat(simple.getIncrement()).isEqualTo(0);
        assertThat(simple.getDecrement()).isEqualTo(10);
    }

    @Test
    public void shouldCreatePNCounterThenGetAndIncrementMultipleTimes() {
        PNCounter simple = CRDT.newPNCounter();
        for ( int i=0; i!=10; ++i ) {
            long value = simple.getAndIncrement();
            assertThat(simple.get()).isEqualTo(value+1);
            assertThat(simple.getIncrement()).isEqualTo(value+1);
            assertThat(simple.getDecrement()).isEqualTo(0);
        }
        assertThat(simple.get()).isEqualTo(10);
        assertThat(simple.getIncrement()).isEqualTo(10);
        assertThat(simple.getDecrement()).isEqualTo(0);
    }

    @Test
    public void shouldCreateTwoPNCountersAndMergeThem() {
        PNCounter c1 = CRDT.newPNCounter(41,22);
        PNCounter c2 = CRDT.newPNCounter(18,20).merge(c1);
        assertThat(c2.get()).isEqualTo(41-22+18-20);
        assertThat(c2.getIncrement()).isEqualTo(41+18);
        assertThat(c2.getDecrement()).isEqualTo(22+20);
    }

    @Test
    public void shouldCreateGCounterAndPNCounterAndMergeThem() {
        GCounter c1 = CRDT.newGCounter(41);
        PNCounter c2 = CRDT.newPNCounter(18,20).merge(c1);
        assertThat(c2.get()).isEqualTo(41-0+18-20);
        assertThat(c2.getIncrement()).isEqualTo(41+18);
        assertThat(c2.getDecrement()).isEqualTo(0+20);
    }
}
