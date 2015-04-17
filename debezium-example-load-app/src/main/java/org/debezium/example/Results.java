/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.example;

public final class Results {

    private final long batchSuccesses;
    private final long batchFailures;
    private final long batchTimeoutFailures;
    private final long patchSuccesses;
    private final long patchRejections;
    private final long patchFailedNoEntities;

    public Results() {
        this.batchSuccesses = 0L;
        this.batchFailures = 0L;
        this.batchTimeoutFailures = 0L;
        this.patchSuccesses = 0L;
        this.patchRejections = 0L;
        this.patchFailedNoEntities = 0L;
    }

    public Results(long batchSuccesses, long batchFailures, long batchTimeouts, long patchSuccesses, long patchRejections,
            long patchFailedNoEntities) {
        this.batchSuccesses = batchSuccesses;
        this.batchFailures = batchFailures;
        this.batchTimeoutFailures = batchTimeouts;
        this.patchSuccesses = patchSuccesses;
        this.patchRejections = patchRejections;
        this.patchFailedNoEntities = patchFailedNoEntities;
    }

    public Results add(Results that) {
        if ( that == null ) return this;
        return new Results(this.batchSuccesses + that.batchSuccesses,
                this.batchFailures + that.batchFailures,
                this.batchTimeoutFailures + that.batchTimeoutFailures,
                this.patchSuccesses + that.patchSuccesses,
                this.patchRejections + that.patchRejections,
                this.patchFailedNoEntities + that.patchFailedNoEntities);
    }
    
    public static Results combine( Results r1, Results r2 ) {
        return r1 == null ? r2 : r1.add(r2);
    }

    @Override
    public String toString() {
        return "Total number of batches:          " + (batchSuccesses + batchFailures + batchTimeoutFailures) + "\n" +
                "  Successful batches:                   " + batchSuccesses + "\n" +
                "  Failed batches:                       " + batchFailures + "\n" +
                "  Timed-out batches:                    " + batchTimeoutFailures + "\n" +
                "Total number of patches:         " + (patchSuccesses + patchRejections + patchFailedNoEntities ) + "\n" +
                "  Successful patches:                   " + patchSuccesses + "\n" +
                "  Rejected patches:                     " + patchRejections + "\n" +
                "  Patches rejected due to no entity:    " + patchFailedNoEntities + "\n";
    }
}