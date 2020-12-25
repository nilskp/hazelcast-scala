package com.hazelcast.Scala

import com.hazelcast.config.{ EvictionConfig, MaxSizePolicy }
import MaxSizePolicy._
import com.hazelcast.memory.MemorySize

sealed abstract class MaxSize(size: Int, maxSizePolicy: MaxSizePolicy) {
  def toConfig(
      policy: EvictionPolicy = EvictionConfig.DEFAULT_EVICTION_POLICY)
      : EvictionConfig =
    new EvictionConfig()
      .setSize(size)
      .setMaxSizePolicy(maxSizePolicy)
      .setMaxSizePolicy(policy)
}

final case class PerNode(maxEntries: Int)
extends MaxSize(maxEntries, PER_NODE)

final case class PerPartition(maxEntries: Int)
extends MaxSize(maxEntries, PER_PARTITION)

final case class PerCluster(maxEntries: Int)
extends MaxSize(maxEntries, ENTRY_COUNT)

final case class UsedHeapPercentage(percentage: Int)
extends MaxSize(percentage, USED_HEAP_PERCENTAGE)

final case class UsedHeapSize(mem: MemorySize)
extends MaxSize(mem.megaBytes.toInt, USED_HEAP_SIZE)

final case class FreeHeapPercentage(percentage: Int)
extends MaxSize(percentage, FREE_HEAP_PERCENTAGE)

final case class FreeHeapSize(mem: MemorySize)
extends MaxSize(mem.megaBytes.toInt, FREE_HEAP_SIZE)

final case class UsedNativeMemoryPercentage(percentage: Int)
extends MaxSize(percentage, USED_NATIVE_MEMORY_PERCENTAGE)

final case class UsedNativeMemorySize(mem: MemorySize)
extends MaxSize(mem.megaBytes.toInt, USED_NATIVE_MEMORY_SIZE)

final case class FreeNativeMemoryPercentage(percentage: Int)
extends MaxSize(percentage, FREE_NATIVE_MEMORY_PERCENTAGE)

final case class FreeNativeMemorySize(mem: MemorySize)
extends MaxSize(mem.megaBytes.toInt, FREE_NATIVE_MEMORY_SIZE)
