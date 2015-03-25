/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.service;

import org.debezium.core.doc.Document;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

/**
 * @author Randall Hauch
 */
public final class PartsCounter {

    private final Counter[] parts1to9;
    private final Counter parts10to15;
    private final Counter parts16to20;
    private final Counter parts21to30;
    private final Counter parts31to50;
    private final Counter parts50plus;

    public PartsCounter( MetricRegistry metrics, String prefix ) {
        parts1to9 = new Counter[9];
        for ( int i=0; i!=parts1to9.length; ++i ) {
            parts1to9[i] = metrics.counter(prefix + (i+1) + "part" + (i > 0 ? "s" : ""));
        }
        parts10to15 = metrics.counter(prefix + "10-15parts");
        parts16to20 = metrics.counter(prefix + "16-20parts");
        parts21to30 = metrics.counter(prefix + "21-30parts");
        parts31to50 = metrics.counter(prefix + "31-50parts");
        parts50plus = metrics.counter(prefix + "50+parts");
    }
    
    public void record( int partCount ) {
        if ( partCount > 1 ) findCounter(partCount).inc();
    }
    
    public void reset() {
        for ( Counter counter : parts1to9 ) {
            reset(counter);
        }
        reset(parts10to15);
        reset(parts16to20);
        reset(parts21to30);
        reset(parts31to50);
        reset(parts50plus);
    }
    
    public void write( Document document ) {
        for ( int i=0; i!=parts1to9.length; ++i ) {
            document.setNumber("" + (i+1) + "part", parts1to9[i].getCount());
        }
        document.setNumber("10-15parts", parts10to15.getCount());
        document.setNumber("16-20parts", parts16to20.getCount());
        document.setNumber("21-30parts", parts21to30.getCount());
        document.setNumber("31-50parts", parts31to50.getCount());
        document.setNumber("50+parts", parts50plus.getCount());
    }
    
    private Counter findCounter( int partCount ) {
        assert partCount != 0;
        if ( partCount < 10 ) return parts1to9[partCount-1];
        if ( partCount <= 15 ) return parts10to15;
        if ( partCount <= 20 ) return parts16to20;
        if ( partCount <= 30 ) return parts21to30;
        if ( partCount <= 50 ) return parts31to50;
        return parts50plus;
    }
    
    private void reset( Counter counter ) {
        counter.dec(counter.getCount());
    }

}
