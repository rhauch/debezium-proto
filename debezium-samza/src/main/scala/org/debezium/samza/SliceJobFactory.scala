/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.samza

import scala.collection.mutable
import org.apache.samza.util.Logging
import org.apache.samza.SamzaException
import org.apache.samza.config.Config
import org.apache.samza.config.ShellCommandConfig._
import org.apache.samza.config.TaskConfig._
import org.apache.samza.container.{TaskNamesToSystemStreamPartitions, SamzaContainer}
import org.apache.samza.job.{StreamJob, StreamJobFactory}
import org.apache.samza.util.Util
import org.apache.samza.config.JobConfig._

/**
 * Creates a new SliceJob job with the given config. A SliceJob is a special form of StreamJob that operates upon a 
 * predefined subset (or "slice") of partitions. As with other jobs, it is identified uniquely by a job name and a job ID. Multiple
 * non-overlapping slices can be run at the same time via jobs with the same name but different IDs.
 * <p>
 * The SliceJob starts one or more SamzaContainer instances in this process based upon the number of threads specified in the 
 * configuration (e.g., "job.threads"). Each container is run in a separate thread, and all of the system stream partitions that
 * make up this slice will be spread across those containers.
 * <p>
 * The job can be safely restarted with a different number of threads, but changing the partitions in the slice may be difficult
 * or time-consuming if the job uses local state.
 */
class SliceJobFactory extends StreamJobFactory with Logging {
  def getJob(config: Config): StreamJob = {
    val numContainers = config.getInt("job.threads", 1)
    val partitionRange = config.get("job.partition.range","ALL")
    info("Job '%s' will use partitions '%s' and %s threads/containers" format (config.get("job.name"),config.get("job.partition.range"),numContainers))

    // Figure out the task names and partitions for each container in this job (as specified by job name and ID) ...
    val taskToTaskNames: Map[Int, TaskNamesToSystemStreamPartitions] = Util.assignContainerToSSPTaskNames(config, numContainers)
    if (taskToTaskNames.size <= 0) {
      val streams = config.getInputStreams
      throw new SamzaException("No SystemStreamPartitions to process were detected for your input streams in slice %s. It's likely that the system(s) specified don't know about the input streams: %s" format (partitionRange, streams))
    }

    // Give developers a nice friendly warning if they've specified task.opts and are using a threaded job.
    config.getTaskOpts match {
      case Some(taskOpts) => warn("%s was specified in config, but is not being used because job is being executed with SliceJob that does not start another process." format TASK_JVM_OPTS)
      case _ => None
    }

    // Determine all of the change log partitions for each task in this job (as specified by job name and ID) ...
    val taskNameToChangeLogPartitionMapping = Util.getTaskNameToChangeLogPartitionMapping(config, taskToTaskNames).map(kv => kv._1 -> Integer.valueOf(kv._2))

    // Create the list of slice containers ...
    val containers = taskToTaskNames.toList.map{kv => 
      val containerName = "partition-slice-"+kv._1
      new SliceContainer(containerName, SamzaContainer(containerName,kv._2,taskNameToChangeLogPartitionMapping,config))
    }
    
    // Create the job ...
    new SliceJob(containers)
  }
}