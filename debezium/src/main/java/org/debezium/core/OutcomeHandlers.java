/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.debezium.api.Database.Outcome;
import org.debezium.api.Database.OutcomeHandler;

/**
 * @author Randall Hauch
 * @param <ResultType> the type of outcome result
 */
public class OutcomeHandlers<ResultType> {
    
    private final ConcurrentMap<RequestId,OutcomeHandler<ResultType>> handlers = new ConcurrentHashMap<>();
    private final Supplier<RequestId> requestIdSupplier;
    public OutcomeHandlers( Supplier<RequestId> requestIdSupplier ) {
        this.requestIdSupplier = requestIdSupplier;
    }
    
    public RequestId register( OutcomeHandler<ResultType> handler ) {
        RequestId id = requestIdSupplier.get();
        if ( handler != null && handlers.put(id, handler) != null ) {
            assert false : "Should not ever re-register for the same request";
        }
        return id;
    }
    
    public void notify( RequestId id, Outcome<ResultType> outcome ) {
        OutcomeHandler<ResultType> handler = handlers.remove(id);
        if ( handler != null ) {
            handler.handle(outcome);
        }
    }
    
    public void notifyAndRemoveAll( Outcome.Status failureCode, String failureReason, Consumer<Throwable> handlerFailure ) {
        Outcome<ResultType> outcome = new Outcome<ResultType>() {
            @Override
            public Status status() {
                return failureCode;
            }
            
            @Override
            public String failureReason() {
                return failureReason;
            }

            @Override
            public ResultType result() {
                return null;
            }
        };
        try {
            handlers.values().forEach((handler)->{
                try {
                    handler.handle(outcome);
                } catch ( RuntimeException e ) {
                    if ( handlerFailure != null ) handlerFailure.accept(e);
                }
            });
        } finally {
            handlers.clear();
        }
    }
}
