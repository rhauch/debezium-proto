/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.samza

import java.util
import org.apache.samza.SamzaException
import org.apache.samza.container.TaskName
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouper
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouperFactory
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import org.apache.samza.config.Config

object GroupByPartitionRange {

  /**
   * Create a GroupByPartitionRange instance that will accept all of the partitions within the specified range.
   * The supplied string is one of:
   * <ol>
   *   <li>the "ALL" literal when all partitions should be included; or</li>
   *   <li>a single non-negative integer denoting the single partition that should be included; or</li>
   *   <li>a range of the form '<lowestPartitionNumber>:<highestPartitionNumber>', where <code>lowestPartitionNumber</code>
   *       is the number of the smallest partition that should be included, and <code>highestPartitionNumber</code> is
   *       the number of the largest partition that should be included.</li>
   * </ol>
   * @param rangeString the string representation of the partition range
   * @return the {@link org.apache.samza.system.SystemStreamPartitionGrouper} instance that supports the supplied range
   */
  def from( rangeString: String ): GroupByPartitionRange = {
    val AllPartitions = """(?i)ALL""".r
    val NoPartitions = """(?i)(NONE)?""".r
    val SinglePartition = """(\d+)""".r
    val PartitionRange = """(\d+)\:(\d+)""".r
    rangeString.trim match {
      case AllPartitions() => all
      case NoPartitions() => none
      case SinglePartition(num) => single(num.toInt)
      case PartitionRange(first,last) => including(first.toInt,last.toInt)
      case _ => throw new SamzaException("The 'job.partition.range' property value '%s' is invalid, and must be either 'ALL', a single partition number, or a range of the form '<lowestPartitionNumber>:<highestPartitionNumber>'" format rangeString )
    }
  }

  /**
   * Create a GroupByPartitionRange instance that will accept all partitions.
   * @return the {@link org.apache.samza.system.SystemStreamPartitionGrouper} instance that supports all partitions
   */
  def all(): GroupByPartitionRange = {
    new GroupByPartitionRange(p => true)
  }

  /**
   * Create a GroupByPartitionRange instance that will accept no partitions.
   * @return the {@link org.apache.samza.system.SystemStreamPartitionGrouper} instance that supports no partitions
   */
  def none(): GroupByPartitionRange = {
    new GroupByPartitionRange(p => false)
  }

  /**
   * Create a GroupByPartitionRange instance that will accept a single partition with the given number.
   * @return the {@link org.apache.samza.system.SystemStreamPartitionGrouper} instance that supports a single partition
   */
  def single( partitionNumber: Int): GroupByPartitionRange = {
    new GroupByPartitionRange(p => p == partitionNumber)
  }

  /**
   * Create a GroupByPartitionRange instance that will accept all partitions within a given range.
   * @first the number of the smallest partition that should be included
   * @last the number of the largest partition that should be included
   * @return the {@link org.apache.samza.system.SystemStreamPartitionGrouper} instance that supports all partitions
   */
  def including( first: Int, last: Int): GroupByPartitionRange = {
    new GroupByPartitionRange(p => p >= first && p <= last)
  }
}

/**
 * Group the {@link org.apache.samza.system.SystemStreamPartition}s by their Partition based upon a range of partition numbers.
 */
class GroupByPartitionRange( private val range: Int => Boolean ) extends SystemStreamPartitionGrouper {

  override def group(ssps: util.Set[SystemStreamPartition]) = {
    ssps.filter( s => range(s.getPartition.getPartitionId) )
        .groupBy( s => new TaskName("Partition " + s.getPartition.getPartitionId) )
        .map(r => r._1 -> r._2.asJava)
  }
}

/**
 * Factory for {@link GroupByPartitionRange} instances.
 */
class GroupByPartitionRangeFactory extends SystemStreamPartitionGrouperFactory {
  override def getSystemStreamPartitionGrouper(config: Config): SystemStreamPartitionGrouper = {
    val rangeStr = config.get("job.partition.range","ALL");
    GroupByPartitionRange.from(rangeStr)
  }
}
