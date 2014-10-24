/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.component;


/**
 * @param <IdType> the type of identifier
 * @author Randall Hauch
 */
public interface SchemaComponent<IdType extends SchemaComponentId> extends Identified<IdType> {
}
