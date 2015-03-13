/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.samza

import org.apache.samza.util.Logging
import org.apache.samza.job.StreamJob
import org.apache.samza.job.ApplicationStatus
import org.apache.samza.job.ApplicationStatus.New
import org.apache.samza.job.ApplicationStatus.Running
import org.apache.samza.job.ApplicationStatus.SuccessfulFinish
import org.apache.samza.job.ApplicationStatus.UnsuccessfulFinish

class SliceJob(private val containers: Seq[SliceContainer]) extends StreamJob with Logging {

  def submit: StreamJob = {

    // create a non-daemon thread to make job runner block until the job finishes.
    // without this, the proc dies when job runner ends.
    containers.foreach( container => container.start )
    SliceJob.this
  }

  def kill: StreamJob = {
    containers.foreach( container => container.stop )
    SliceJob.this
  }

  def waitForFinish(timeoutMs: Long) = {
    containers.foreach( container => container.waitForFinish(timeoutMs) )
    getStatus()
  }

  def waitForStatus(status: ApplicationStatus, timeoutMs: Long) = {
    val start = System.currentTimeMillis
    while (System.currentTimeMillis - start < timeoutMs && !status.equals(getStatus)) {
      Thread.sleep(500)
    }
    getStatus()
  }

  def getStatus = {
    val statuses = containers.map(container => container.getStatus ).toSet
    if ( statuses.contains(Running) ) Running
    else if ( statuses.contains(UnsuccessfulFinish) ) UnsuccessfulFinish
    else if ( statuses.contains(SuccessfulFinish) ) SuccessfulFinish
    else if ( statuses.contains(New) ) New
    else null
  }
}
