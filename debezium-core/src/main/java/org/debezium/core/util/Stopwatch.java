/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.util;

import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.debezium.core.annotation.ThreadSafe;

/**
 * A stopwatch for measuring durations. All NewStopwatch implementations are threadsafe, although using a single stopwatch
 * object across threads requires caution and care.
 * 
 * @author Randall Hauch
 */
@ThreadSafe
public abstract class Stopwatch {

    /**
     * Start the stopwatch. Calling this method on an already-started stopwatch has no effect.
     * 
     * @return this object to enable chaining methods
     * @see #stop
     */
    public abstract Stopwatch start();

    /**
     * Stop the stopwatch. Calling this method on an already-stopped stopwatch has no effect.
     * 
     * @return this object to enable chaining methods
     * @see #start()
     */
    public abstract Stopwatch stop();

    /**
     * Get the total and average durations measured by this stopwatch.
     * 
     * @return the durations; never null
     */
    public abstract Durations durations();

    /**
     * The average and total durations as measured by one or more stopwatches.
     */
    @ThreadSafe
    public static interface Durations {
        /**
         * Get the total duration that the stopwatch or stopwatches have run.
         * 
         * @return the total duration; never null
         */
        public Duration total();

        /**
         * Get the average duration that the stopwatch or stopwatches have run.
         * 
         * @return the average duration; never null
         */
        public Duration average();

        /**
         * Get the total duration that the stopwatch or stopwatches have run, as a formatted string.
         * 
         * @return the string representation of the total duration; never null
         */
        default public String totalAsString() {
            return asString(total());
        }

        /**
         * Get the average duration that the stopwatch or stopwatches have run, as a formatted string.
         * 
         * @return the string representation of the average duration; never null
         */
        default public String averageAsString() {
            return asString(average());
        }
        
        /**
         * Atomically obtain a snapshot of the durations that will not be mutable.
         * @return the immutable snapshot of this object; never null
         */
        public Durations snapshot();
    }

    /**
     * Create a new {@link Stopwatch} that can be reused. The resulting {@link Stopwatch#durations()}, however,
     * only reflect the most recently completed stopwatch interval.
     * <p>
     * For example, the following code shows this behavior:
     * 
     * <pre>
     * Stopwatch sw = Stopwatch.reusable();
     * sw.start();
     * sleep(3000); // sleep 3 seconds
     * sw.stop();
     * print(sw.durations()); // total and average duration are each 3 seconds
     * 
     * sw.start();
     * sleep(2000); // sleep 2 seconds
     * sw.stop();
     * print(sw.durations()); // total and average duration are each 2 seconds
     * </pre>
     * 
     * @return the new stopwatch; never null
     */
    public static Stopwatch reusable() {
        return createWith(new SingleDuration(), null, null);
    }

    /**
     * Create a new {@link Stopwatch} that records all of the measured durations of the stopwatch.
     * <p>
     * For example, the following code shows this behavior:
     * 
     * <pre>
     * Stopwatch sw = Stopwatch.accumulating();
     * sw.start();
     * sleep(3000); // sleep 3 seconds
     * sw.stop();
     * print(sw.durations()); // total and average duration are each 3 seconds
     * 
     * sw.start();
     * sleep(2000); // sleep 2 seconds
     * sw.stop();
     * print(sw.durations()); // total duration is now 5 seconds, average is 2.5 seconds
     * </pre>
     * 
     * @return the new stopwatch; never null
     */
    public static Stopwatch accumulating() {
        return createWith(new MultipleDurations(), null, null);
    }

    /**
     * A set of stopwatches whose durations are combined. New stopwatches can be created at any time, and when
     * {@link Stopwatch#stop()} will always record their duration with this set.
     * <p>
     * This set is threadsafe, meaning that multiple threads can {@link #create()} new stopwatches concurrently, and each
     * stopwatch's duration is measured separately. Additionally, all of the other methods of this interface are also threadsafe.
     * </p>
     * 
     * @author Randall Hauch
     */
    @ThreadSafe
    public static interface StopwatchSet extends Durations {
        /**
         * Create a new stopwatch that records durations with this set.
         * 
         * @return the new stopwatch; never null
         */
        Stopwatch create();

        /**
         * Block until all running stopwatches have been {@link Stopwatch#stop() stopped}. This means that if a stopwatch
         * is {@link #create() created} but never started, this method will not wait for it. Likewise, if a stopwatch
         * is {@link #create() created} and started, then this method will block until the stopwatch is
         * {@link Stopwatch#stop() stopped} (even if the same stopwatch is started multiple times).
         * are stopped.
         * 
         * @throws InterruptedException if the thread is interrupted before unblocking
         */
        void await() throws InterruptedException;

        /**
         * Block until all stopwatches that have been {@link #create() created} and {@link Stopwatch#start() started} are
         * stopped.
         * 
         * @param timeout the maximum length of time that this method should block
         * @param unit the unit for the timeout; may not be null
         * @throws InterruptedException if the thread is interrupted before unblocking
         */
        void await(long timeout, TimeUnit unit) throws InterruptedException;
    }

    /**
     * Create a new set of stopwatches. The resulting object is threadsafe.
     * 
     * @return the stopwatches object; never null
     */
    public static StopwatchSet multiple() {
        MultipleDurations durations = new MultipleDurations();
        VariableLatch latch = new VariableLatch(0);
        return new StopwatchSet() {
            @Override
            public Stopwatch create() {
                return createWith(durations, latch::countUp, latch::countDown);
            }

            @Override
            public Duration average() {
                return durations.average();
            }

            @Override
            public Duration total() {
                return durations.total();
            }
            
            @Override
            public Durations snapshot() {
                return durations.snapshot();
            }

            @Override
            public void await() throws InterruptedException {
                latch.await();
            }

            @Override
            public void await(long timeout, TimeUnit unit) throws InterruptedException {
                latch.await(timeout, unit);
            }
        };
    }

    /**
     * Compute the readable string representation of the supplied duration.
     * 
     * @param duration the duration; may not be null
     * @return the string representation; never null
     */
    protected static String asString(Duration duration) {
        return duration.toString().substring(2);
    }

    /**
     * Create a new stopwatch that updates the given {@link BaseDurations duration}, and optionally has functions to
     * be called after the stopwatch is started and stopped.
     * <p>
     * The resulting stopwatch is threadsafe.
     * </p>
     * 
     * @param duration the duration that should be updated; may not be null
     * @param uponStart the function that should be called when the stopwatch is successfully started (after not running); may be
     *            null
     * @param uponStop the function that should be called when the stopwatch is successfully stopped (after it was running); may
     *            be null
     * @return the new stopwatch
     */
    protected static Stopwatch createWith(BaseDurations duration, Runnable uponStart, Runnable uponStop) {
        return new Stopwatch() {
            private final AtomicLong started = new AtomicLong(0L);

            @Override
            public Stopwatch start() {
                started.getAndUpdate(existing -> {
                    if (existing == 0L) {
                        // Has not yet been started ...
                        existing = System.nanoTime();
                        if (uponStart != null) uponStart.run();
                    }
                    return existing;
                });
                return this;
            }

            @Override
            public Stopwatch stop() {
                started.getAndUpdate(existing -> {
                    if (existing != 0L) {
                        // Is running but has not yet been stopped ...
                        duration.add(Duration.ofNanos(System.nanoTime() - existing));
                        if (uponStop != null) uponStop.run();
                        return 0L;
                    }
                    return existing;
                });
                return this;
            }

            @Override
            public Durations durations() {
                return duration;
            }
        };
    }

    /**
     * Abstract base class for {@link Durations} implementations.
     * 
     * @author Randall Hauch
     */
    @ThreadSafe
    protected static abstract class BaseDurations implements Durations {
        public abstract void add(Duration duration);

        @Override
        public String toString() {
            return "Total: " + totalAsString() + "; average: " + averageAsString();
        }
    }

    /**
     * A {@link Durations} implementation that only remembers the most recently {@link #add(Duration) added} duration.
     * 
     * @author Randall Hauch
     */
    @ThreadSafe
    private static final class SingleDuration extends BaseDurations {
        private final AtomicLong total = new AtomicLong();

        @Override
        public Duration total() {
            return Duration.ofNanos(total.get());
        }

        @Override
        public Duration average() {
            return total();
        }

        @Override
        public void add(Duration duration) {
            total.set(duration.toNanos());
        }
        
        @Override
        public Durations snapshot() {
            SingleDuration result = new SingleDuration();
            result.add(total());
            return result;
        }
    }

    /**
     * A {@link Durations} implementation that accumulates all {@link #add(Duration) added} durations.
     * 
     * @author Randall Hauch
     */
    @ThreadSafe
    private static final class MultipleDurations extends BaseDurations {
        private final ConcurrentLinkedQueue<Duration> durations = new ConcurrentLinkedQueue<>();

        @Override
        public Duration total() {
            AtomicLong totalDuration = new AtomicLong();
            durations.forEach(duration -> totalDuration.addAndGet(duration.toNanos()));
            return Duration.ofNanos(totalDuration.get());
        }

        @Override
        public Duration average() {
            return computeAverage(new AtomicLong());
        }

        public Duration computeAverage(AtomicLong count) {
            count.set(0);
            AtomicLong totalDuration = new AtomicLong();
            durations.forEach(duration -> {
                totalDuration.addAndGet(duration.toNanos());
                count.incrementAndGet();
            });
            return count.get() == 0 ? Duration.ZERO : Duration.ofNanos(totalDuration.get() / count.get());
        }

        @Override
        public void add(Duration duration) {
            durations.add(duration);
        }
        
        @Override
        public Durations snapshot() {
            MultipleDurations result = new MultipleDurations();
            durations.forEach(result::add);
            return result;
        }
        
        @Override
        public String toString() {
            AtomicLong count = new AtomicLong();
            Duration avg = computeAverage(count);
            return "Total: " + totalAsString() + "; average: " + asString(avg) + "; count=" + count.get();
        }
    }
}
