/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.model;

import org.debezium.message.Patch.Action;

/**
 * The status of a requested change operation.
 * 
 * @author Randall Hauch
 */
public enum ChangeStatus {
    /** The change was successfully applied. */
    OK,
    /** The requested change could not be made because the target of the change no longer exists. */
    DOES_NOT_EXIST,
    /**
     * The requested change could not be made because at least one {@link Action#REQUIRE requirement} within the patch
     * could not be satisfied.
     */
    PATCH_FAILED;
}