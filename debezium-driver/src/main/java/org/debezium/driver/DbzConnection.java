/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import org.debezium.core.component.DatabaseId;
import org.debezium.core.component.Entity;
import org.debezium.core.component.EntityId;
import org.debezium.core.component.Identified;
import org.debezium.core.component.Identifier;
import org.debezium.core.component.Schema;
import org.debezium.core.doc.Document;
import org.debezium.core.function.Callable;
import org.debezium.core.message.Batch;
import org.debezium.core.message.Message;
import org.debezium.core.message.Message.Status;
import org.debezium.core.message.Patch;

/**
 * A lightweight connection to a database.
 * 
 * @author Randall Hauch
 *
 */
final class DbzConnection implements Database {

    private final DbzDatabases dbs;
    private final ExecutionContext context;
    private volatile boolean isClosed = false;

    DbzConnection(DbzDatabases dbs,
            ExecutionContext context) {
        this.dbs = dbs;
        this.context = context;
        assert context != null;
        assert dbs != null;
    }

    ExecutionContext getContext() {
        return context;
    }

    @Override
    public DatabaseId databaseId() {
        return context.databaseId();
    }

    @Override
    public boolean isConnected() {
        return !isClosed;
    }

    @Override
    public synchronized void close() {
        if (!isClosed && dbs.disconnect(this)) {
            isClosed = true;
        }
    }

    private void ensureOpen() {
        if (isClosed) throw new IllegalStateException("This database connection for " + context + " is closed");
    }

    @Override
    public Completion readSchema(OutcomeHandler<Schema> handler) {
        ensureOpen();
        RequestCompletion latch = new RequestCompletion();
        dbs.readSchema(context, handle(handler, latch, response -> Schema.with(context.databaseId(), response)));
        return latch;
    }

    @Override
    public Completion readEntities(Iterable<EntityId> entityIds, OutcomeHandler<Stream<Entity>> handler) {
        ensureOpen();
        if (!entityIds.iterator().hasNext()) throw new DebeziumInvalidRequestException("The batch is empty");
        RequestCompletion latch = new RequestCompletion();
        dbs.readEntities(context, entityIds, handleStream(handler, latch, response -> {
            EntityId id = Message.getEntityId(response);
            Document representation = Message.getAfter(response);
            if (representation != null) return Entity.with(id, representation);
            changeStatus(Status.DOES_NOT_EXIST);
            return null;
        }));
        return latch;
    }

    @Override
    public Completion changeEntities(Batch<EntityId> batch, OutcomeHandler<Stream<Change<EntityId, Entity>>> handler) {
        ensureOpen();
        if (batch.isEmpty()) throw new DebeziumInvalidRequestException("The batch is empty");
        RequestCompletion latch = new RequestCompletion();
        dbs.changeEntities(context, batch, handleStream(handler, latch, response -> {
            EntityId id = Message.getEntityId(response);
            Document entity = Message.getAfterOrBefore(response);
            ChangeStatus status = changeStatus(Message.getStatus(response));
            Patch<EntityId> patch = Patch.forEntity(response);
            Collection<String> failureReasons = Message.getFailureReasons(response);
            return changed(status, id, Entity.with(id, entity), patch, failureReasons);
        }));
        return latch;
    }

    private static <R> ResponseHandlers.Handlers handleStream(OutcomeHandler<Stream<R>> handler, RequestCompletion latch,
                                                              Function<Document, R> processResponse) {
        StreamOutcomeBuilder<R> builder = new StreamOutcomeBuilder<R>(handler, latch::complete, processResponse);
        return ResponseHandlers.with(builder::accumulate, builder::success, builder::failed);
    }

    private static <R> ResponseHandlers.Handlers handle(OutcomeHandler<R> handler, RequestCompletion latch,
                                                        Function<Document, R> processResponse) {
        SingleOutcomeBuilder<R> builder = new SingleOutcomeBuilder<R>(handler, latch::complete, processResponse);
        return ResponseHandlers.with(builder::set, builder::success, builder::failed);
    }

    private static ChangeStatus changeStatus(Message.Status messageStatus) {
        switch (messageStatus) {
            case SUCCESS:
                return ChangeStatus.OK;
            case PATCH_FAILED:
                return ChangeStatus.PATCH_FAILED;
            case DOES_NOT_EXIST:
                return ChangeStatus.DOES_NOT_EXIST;
        }
        throw new IllegalStateException("Unknown status: " + messageStatus);
    }

    private static final class RequestCompletion implements Completion {
        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public boolean isComplete() {
            return latch.getCount() == 0;
        }

        @Override
        public void await() throws InterruptedException {
            latch.await();
        }

        @Override
        public void await(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
            latch.await(timeout, unit);
        }

        protected RequestCompletion complete() {
            latch.countDown();
            return this;
        }
    }

    private static final class StreamOutcomeBuilder<T> {
        private final OutcomeHandler<Stream<T>> handler;
        private final List<T> results = new ArrayList<>();
        private final Function<Document, T> responseAdapter;
        private final Callable completion;

        protected StreamOutcomeBuilder(OutcomeHandler<Stream<T>> handler, Callable completion, Function<Document, T> adapter) {
            this.responseAdapter = adapter;
            this.handler = handler;
            this.completion = completion;
        }

        public void accumulate(Document response) {
            T result = responseAdapter.apply(response);
            if (result != null) {
                results.add(result);
            }
        }

        public void success() {
            try {
                if (handler != null) {
                    handler.handle(new Outcome<Stream<T>>() {
                        @Override
                        public Stream<T> result() {
                            return results.stream();
                        }

                        @Override
                        public Outcome.Status status() {
                            return Outcome.Status.OK;
                        }

                        @Override
                        public String failureReason() {
                            return null;
                        }
                    });
                }
            } finally {
                if (completion != null) {
                    completion.call();
                }
            }
        }

        public void failed(Outcome.Status status, String failureReason) {
            try {
                if (handler != null) {
                    handler.handle(new Outcome<Stream<T>>() {
                        @Override
                        public Stream<T> result() {
                            return null;
                        }

                        @Override
                        public Outcome.Status status() {
                            return status;
                        }

                        @Override
                        public String failureReason() {
                            return failureReason;
                        }
                    });
                }
            } finally {
                if (completion != null) completion.call();
            }
        }
    }

    private static final class SingleOutcomeBuilder<T> {
        private final OutcomeHandler<T> handler;
        private final AtomicReference<T> results = new AtomicReference<>();
        private final Function<Document, T> responseAdapter;
        private final Callable completion;

        protected SingleOutcomeBuilder(OutcomeHandler<T> handler, Callable completion, Function<Document, T> adapter) {
            this.responseAdapter = adapter;
            this.handler = handler;
            this.completion = completion;
        }

        public void set(Document response) {
            results.set(responseAdapter.apply(response));
        }

        public void success() {
            try {
                if (handler != null) {
                    handler.handle(new Outcome<T>() {
                        @Override
                        public T result() {
                            return results.get();
                        }

                        @Override
                        public Outcome.Status status() {
                            return Outcome.Status.OK;
                        }

                        @Override
                        public String failureReason() {
                            return null;
                        }
                    });
                }
            } finally {
                if (completion != null) completion.call();
            }
        }

        public void failed(Outcome.Status status, String failureReason) {
            try {
                if (handler != null) {
                    handler.handle(new Outcome<T>() {
                        @Override
                        public T result() {
                            return null;
                        }

                        @Override
                        public Outcome.Status status() {
                            return status;
                        }

                        @Override
                        public String failureReason() {
                            return failureReason;
                        }
                    });
                }
            } finally {
                if (completion != null) completion.call();
            }
        }
    }

    private static <I extends Identifier, T extends Identified<I>> Changed<I, T> changed(ChangeStatus status, I id, T target,
                                                                                         Patch<I> patch, Collection<String> failureReasons) {
        return new Changed<I, T>(status, id, target, patch, failureReasons);
    }

    private static final class Changed<I extends Identifier, T extends Identified<I>> implements Change<I, T> {

        private final I id;
        private final ChangeStatus status;
        private final T target;
        private final Collection<String> failureReasons;
        private final Patch<I> patch;

        protected Changed(ChangeStatus status, I id, T target, Patch<I> patch, Collection<String> failureReasons) {
            this.id = id;
            this.status = status;
            this.target = target;
            this.patch = patch;
            this.failureReasons = failureReasons;
        }

        @Override
        public I id() {
            return id;
        }

        @Override
        public ChangeStatus status() {
            return status;
        }

        @Override
        public T target() {
            return target;
        }

        @Override
        public Patch<I> patch() {
            return patch;
        }

        @Override
        public Stream<String> failureReasons() {
            return failureReasons.stream();
        }
    }
}