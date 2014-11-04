/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.client;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.debezium.client.Database.Outcome;
import org.debezium.client.Database.OutcomeHandler;
import org.debezium.core.doc.Document;

/**
 * A collection of {@link OutcomeHandler} instances keyed by {@link RequestId}.
 * 
 * @author Randall Hauch
 */
final class OutcomeHandlers {
    
    public static interface ResponseHandler {
        void handleResponse( Document response );
        void handleError( Outcome.Status status, String failureReason );
    }
    
    protected static final class Registration {
        protected final ExecutionContext context;
        protected final ResponseHandler handler;
        
        protected Registration(ExecutionContext context, ResponseHandler handler) {
            this.context = context;
            this.handler = handler;
        }
    }
    
    private final ConcurrentMap<RequestId, Registration> responseConsumers = new ConcurrentHashMap<>();
    private final Supplier<RequestId> requestIdSupplier;
    /**
     * A read-write lock used to ensure that new handlers cannot be {@link #register(ExecutionContext, ResponseHandler)
     * registered} when the existing handlers are
     * {@link #notifyAndRemoveAll(org.debezium.client.Database.Outcome.Status, String, Consumer) removed}. The use of a read-write
     * lock means that registering a new handler (via the read lock) can be done concurrently. Note that
     * {@link #notify(RequestId, Document) handling response messages} will atomically remove the registrations
     * and therefore does not need to use the lock.
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    
    public OutcomeHandlers(Supplier<RequestId> requestIdSupplier) {
        this.requestIdSupplier = requestIdSupplier;
    }
    
    public RequestId register(ExecutionContext context, ResponseHandler handler) {
        assert context != null;
        assert handler != null;
        RequestId id = requestIdSupplier.get();
        try {
            lock.readLock().lock();
            responseConsumers.put(id, new Registration(context, handler));
        } finally {
            lock.readLock().unlock();
        }
        return id;
    }
    
    public <T> void notify(RequestId id, Document responseMessage) {
        Registration registration = responseConsumers.remove(id);
        if (registration != null) {
            registration.handler.handleResponse(responseMessage);
        }
    }
    
    public void notifyAndRemoveAll(Outcome.Status failureCode, String failureReason, Consumer<Throwable> handlerFailure) {
        try {
            lock.writeLock().lock();
            try {
                responseConsumers.entrySet().forEach((entry) -> {
                    Registration reg = entry.getValue();
                    try {
                        reg.handler.handleError(failureCode, failureReason);
                    } catch (RuntimeException e) {
                        if (handlerFailure != null) handlerFailure.accept(e);
                    }
                });
            } finally {
                responseConsumers.clear();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
