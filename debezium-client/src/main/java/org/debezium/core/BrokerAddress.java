/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core;

import java.util.Optional;

/**
 * @author Randall Hauch
 *
 */
final class BrokerAddress {

    public static BrokerAddress parse( String brokerString ) {
        String[] parts = brokerString.split(":");
        if ( parts.length == 2 && isInteger(parts[1])) {
            return new BrokerAddress(parts[0],null,Integer.parseInt(parts[1]));
        } else if ( parts.length == 3 && isInteger(parts[2])) {
            return new BrokerAddress(parts[0],parts[1],Integer.parseInt(parts[3]));
        }
        throw new IllegalArgumentException("Unknown format for Kafka broker string: '" + brokerString + "'");
    }
    
    private static boolean isInteger( String integerString ) {
        try {
            Integer.parseInt(integerString);
            return true;
        } catch ( NumberFormatException e ) {
            return false;
        }
    }

    private final String brokerName;
    private final Optional<String> machine;
    private final int port;
    protected BrokerAddress( String brokerName, String machine, int port ) {
        this.brokerName = brokerName;
        this.machine = Optional.ofNullable(machine);
        this.port = port;
    }
    @Override
    public int hashCode() {
        return brokerName.hashCode();
    }
    @Override
    public boolean equals(Object obj) {
        if ( obj == this ) return true;
        if ( obj instanceof BrokerAddress ) {
            BrokerAddress that = (BrokerAddress)obj;
            return this.brokerName.equals(that.brokerName) &&
                   this.machine.equals(that.machine) &&
                   this.port == that.port;
        }
        return false;
    }
    @Override
    public String toString() {
        return brokerName + ":" + ( machine.isPresent() ? machine.get() + ":" : "" ) + Integer.toString(port);
    }
}
