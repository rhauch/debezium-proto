/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium;

import java.text.DecimalFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * @author Randall Hauch
 *
 */
public abstract class Stopwatch {
    
    protected static class SimpleStopwatch extends Stopwatch {
        protected long started = 0L;
        @Override
        public Stopwatch start() {
            duration = null;
            started = System.currentTimeMillis();
            return this;
        }
        @Override
        public Stopwatch stop() {
            duration = Duration.ofMillis(System.currentTimeMillis() - started);
            return this;
        }
        
        @Override
        public Duration totalDuration() {
            return duration;
        }
    };

    protected static class RestartableStopwatch extends SimpleStopwatch {
        private int count = 0;
        @Override
        public Stopwatch start() {
            ++count;
            super.start();
            return this;
        }
        @Override
        public Stopwatch stop() {
            super.stop();
            if ( duration == null ) duration = Duration.ZERO;
            duration = duration.plus(System.currentTimeMillis() - started,ChronoUnit.MILLIS);
            return this;
        }

        @Override
        public Duration averageDuration(int count) {
            return super.averageDuration(this.count * count);
        }
    };

    protected final DecimalFormat format = new DecimalFormat("0.0######");
    protected Duration duration;
    
    public static Stopwatch simple() {
        return new SimpleStopwatch();
    }
    
    public static Stopwatch restartable() {
        return new RestartableStopwatch();
    }

    public abstract Stopwatch start();
    
    public abstract Stopwatch stop();
    
    public Stopwatch reset() {
        duration = null;
        return this;
    }
    
    public Duration totalDuration() {
        return duration != null ? duration : Duration.ZERO;
    }
    
    public Duration averageDuration( int count ) {
        return count < 1 ? Duration.ofSeconds(0) : duration.dividedBy(count);
    }
    
    @Override
    public String toString() {
        return totalDuration().toString();
    }
    
}
