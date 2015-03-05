/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.samza;

import org.apache.samza.config.Config;
import org.apache.samza.job.StreamJob;
import org.apache.samza.job.StreamJobFactory;

/**
 * A special {@link StreamJobFactory} implementation that produces {@link StreamJob}s that each runs a SamzaContainer
 * with tasks for a slice of partitions for the input streams. This is useful when the slices are known a priori
 * and jobs are run on different machines each with a slice of a subset of the partitions.
 * 
 * @author Randall Hauch
 *
 */
public class SliceJobFactory implements StreamJobFactory {

    @Override
    public StreamJob getJob(Config config) {
        return null;
    }

}
