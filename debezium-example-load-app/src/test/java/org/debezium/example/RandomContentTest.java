/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.example;

import java.text.DecimalFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.DoubleAccumulator;

import org.debezium.Testing;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.EntityType;
import org.debezium.core.component.Identifier;
import org.debezium.core.message.Batch;
import org.debezium.core.message.Patch;
import org.debezium.core.util.Stopwatch;
import org.debezium.example.RandomContent.ContentGenerator;
import org.debezium.example.RandomContent.IdGenerator;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class RandomContentTest implements Testing {

    private static final EntityType CONTACT_TYPE = Identifier.of("myDB", "contact");
    private static RandomContent content;
    private ContentGenerator generator;

    @BeforeClass
    public static void beforeAll() {
        content = RandomContent.load();
    }

    @Before
    public void beforeEach() {
        generator = content.createGenerator();
    }

    @Test
    public void shouldGenerateEmptyBatchWhenNoEntityIdsAreSupplied() {
        Batch<EntityId> batch = generator.generateBatch(null, null);
        assertThat(batch.isEmpty()).isTrue();
    }

    @Test
    public void shouldGenerateBatchWithOneEditedEntityAndNoRemovedEntities() {
        Batch<EntityId> batch = generateBatch(1, 0, CONTACT_TYPE);
        assertThat(batch.isEmpty()).isFalse();
        assertThat(batch.patchCount()).isEqualTo(1);
        print(batch.patch(0));
    }

    @Test
    public void shouldGenerateBatchWithMultipleEditedEntitiesAndNoRemovedEntities() {
        Batch<EntityId> batch = generateBatch(10, 0, CONTACT_TYPE);
        assertThat(batch.isEmpty()).isFalse();
        assertThat(batch.patchCount()).isEqualTo(10);
        print(batch);
    }

    @Test
    public void shouldGenerateBatchWithZeroEditedEntitiesAndOneRemovedEntity() {
        Batch<EntityId> batch = generateBatch(0, 1, CONTACT_TYPE);
        assertThat(batch.isEmpty()).isFalse();
        assertThat(batch.patchCount()).isEqualTo(1);
        print(batch);
    }

    @Test
    public void shouldGenerateBatchWithZeroEditedEntitiesAndMultipleRemovedEntity() {
        Batch<EntityId> batch = generateBatch(0, 10, CONTACT_TYPE);
        assertThat(batch.isEmpty()).isFalse();
        assertThat(batch.patchCount()).isEqualTo(10);
        print(batch);
    }

    @Test
    public void shouldGenerateBatchWithOneEditedEntityAndOneRemovedEntities() {
        Batch<EntityId> batch = generateBatch(1, 1, CONTACT_TYPE);
        assertThat(batch.isEmpty()).isFalse();
        assertThat(batch.patchCount()).isEqualTo(2);
        print(batch.patch(0));
    }

    @Test
    public void shouldGenerateBatchWithMultipleEditedEntitiesAndMultipleRemovedEntities() {
        Batch<EntityId> batch = generateBatch(10, 10, CONTACT_TYPE);
        assertThat(batch.isEmpty()).isFalse();
        assertThat(batch.patchCount()).isEqualTo(20);
        // Testing.Print.enable();
        print(batch);
    }

    @Test
    public void shouldGenerateManyBatches() {
        generateBatches(null, 100);
        Stopwatch sw = Stopwatch.simple();
        generateBatches(sw, 1000);
        Testing.Print.enable();
        Testing.print("1000 batches in " + asString(sw.totalDuration()) + " (" + asString(sw.averageDuration(1000)) + " per batch)");
    }

    @Test
    public void shouldAllowMultipleThreadsToEachUseGenerator() {
        Testing.Print.enable();
        int numThreads = 1;
        int numberOfWarmupBatches =1000;
        int numBatchesPerThread = 300000;
        CountDownLatch starter = new CountDownLatch(1);
        CountDownLatch stopper = new CountDownLatch(numThreads);
        DoubleAccumulator totalBatchesPerSecond = new DoubleAccumulator(this::add,0.0d);
        for (int i = 0; i != numThreads; ++i) {
            String threadName = "Thread" + (i + 1);
            Thread t = new Thread(() -> {
                Stopwatch sw = Stopwatch.restartable();
                try {
                    generateBatches(null, numberOfWarmupBatches);
                    Testing.print("Waiting to start " + threadName);
                    starter.await();
                    generateBatches(sw, numBatchesPerThread);
                    Testing.print(threadName + " generated " + numBatchesPerThread + " batches in " + total(sw) + " ("
                            + batchesPerSecondString(sw,numBatchesPerThread) + " batches/sec)");
                    totalBatchesPerSecond.accumulate(batchesPerSecond(sw,numBatchesPerThread));
                } catch (InterruptedException e) {
                    Testing.printError(e);
                    Thread.interrupted();
                } finally {
                    stopper.countDown();
                }
            });
            t.start();
        }
        starter.countDown();
        try {
            stopper.await(10, TimeUnit.SECONDS);
            Testing.print("Completed all threads, total batches/sec: " + new DecimalFormat("0.00").format(totalBatchesPerSecond.doubleValue()));
        } catch (InterruptedException e) {
            Testing.printError(e);
            Thread.interrupted();
        }
    }
    
    protected double add(double left, double right) {
        return left + right;
    }
    
    protected String total( Stopwatch sw ) {
        return asString(sw.totalDuration());
    }
    
    protected String average( Stopwatch sw, int count) {
        return asString(sw.averageDuration(count));
    }
    
    protected String batchesPerSecondString( Stopwatch sw, int totalCount ) {
        return new DecimalFormat("0.0##").format(batchesPerSecond(sw,totalCount));
    }
    
    protected double batchesPerSecond( Stopwatch sw, int totalCount ) {
        Duration duration = sw.totalDuration();
        double seconds = duration.getSeconds() + (duration.getNano() / 1e9);
        return totalCount / seconds;
    }
    
    protected String asString( Duration duration ) {
        return duration.toString().substring(2);
    }
    
    protected double invert( Duration duration ) {
        return 1.0d / duration.get(ChronoUnit.NANOS);
    }

    protected void generateBatches(Stopwatch sw, int numBatches) {
        IdGenerator idGenerator = generateIds(5, 2, CONTACT_TYPE);
        try {
            Batch<EntityId> batch = null;
            if (sw != null ) sw.start();
            for (int i = 0; i != numBatches; ++i) {
                batch = generator.generateBatch(idGenerator);
            }
            assertThat(batch).isNotNull();
        } finally {
            if (sw != null ) sw.stop();
        }
    }

    protected Batch<EntityId> generateBatch(int editCount, int removeCount, EntityType type) {
        return generator.generateBatch(generateIds(editCount, removeCount, type));
    }

    protected IdGenerator generateIds(int editCount, int removeCount, EntityType type) {
        return new IdGenerator() {
            @Override
            public EntityId[] generateEditableIds() {
                return generateIds(editCount, type);
            }

            @Override
            public EntityId[] generateRemovableIds() {
                return generateIds(removeCount, type);
            }
        };
    }

    private EntityId[] generateIds(int count, EntityType type) {
        if (count <= 0) return null;
        EntityId[] ids = new EntityId[count];
        for (int i = 0; i != count; ++i) {
            ids[i] = Identifier.of(type,Integer.toString(count));
        }
        return ids;
    }

    protected void print(Batch<?> batch) {
        if (Print.isEnabled()) batch.forEach(Testing::print);
    }

    protected void print(Patch<?> patch) {
        Testing.print(patch);
    }

}
