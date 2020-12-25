package com.hazelcast.Scala

import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import com.hazelcast.client.config.ClientConfig

import com.hazelcast.config.Config
import com.hazelcast.partition.PartitioningStrategy
import com.hazelcast.internal.diagnostics.HealthMonitorLevel
import com.hazelcast.memory.MemorySize
import com.hazelcast.query.impl.predicates.QueryOptimizerFactory
import com.hazelcast.spi.properties.HazelcastProperty

sealed abstract class HzProperties[C <: { def setProperty(k: String, v: String): C }](conf: C) {
  import language.reflectiveCalls
  import com.hazelcast.spi.properties.ClusterProperty._
  protected final def set(key: HazelcastProperty, value: Any): C = value match {
    case null => conf.setProperty(key.getName, null)
    case _ => conf.setProperty(key.getName, value.toString)
  }
  @deprecated("deprecate caller", since="3.9")
  protected final def set(key: String, value: Any): C = value match {
    case null => conf.setProperty(key, null)
    case _ => conf.setProperty(key, value.toString)
  }
  /** @see com.hazelcast.client.properties.ClientProperty.LOGGING_TYPE */
  def setLoggingType(lt: String): C = set(LOGGING_TYPE, lt)

  /** @see com.hazelcast.client.properties.ClientProperty.DISCOVERY_SPI_ENABLED */
  def setDiscoverySPIEnabled(enabled: Boolean): C = set(DISCOVERY_SPI_ENABLED, enabled)
  /** @see com.hazelcast.client.properties.ClientProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED */
  def setDiscoverySPIPublicIPEnabled(enabled: Boolean): C = set(DISCOVERY_SPI_PUBLIC_IP_ENABLED, enabled)
}

class HzClientProperties(conf: ClientConfig) extends HzProperties(conf) {
  import com.hazelcast.client.properties.ClientProperty._

  /** @see com.hazelcast.client.spi.properties.ClientProperty.EVENT_QUEUE_CAPACITY */
  def setEventQueueCapacity(cap: Int): ClientConfig = set(EVENT_QUEUE_CAPACITY, cap)
  /** @see com.hazelcast.client.spi.properties.ClientProperty.EVENT_THREAD_COUNT */
  def setEventThreadCount(threads: Int): ClientConfig = set(EVENT_THREAD_COUNT, threads)
  /** @see com.hazelcast.client.spi.properties.ClientProperty.HEARTBEAT_INTERVAL */
  def setHeartbeatInterval(interval: FiniteDuration): ClientConfig = set(HEARTBEAT_INTERVAL, interval.toMillis)
  /** @see com.hazelcast.client.spi.properties.ClientProperty.HEARTBEAT_TIMEOUT */
  def setHeartbeatTimeout(timeout: FiniteDuration): ClientConfig = set(HEARTBEAT_TIMEOUT, timeout.toMillis)
  /** @see com.hazelcast.client.spi.properties.ClientProperty.INVOCATION_TIMEOUT_SECONDS */
  def setInvocationTimeout(timeout: FiniteDuration): ClientConfig = set(INVOCATION_TIMEOUT_SECONDS, timeout.toSeconds)
  /** @see com.hazelcast.client.spi.properties.ClientProperty.SHUFFLE_MEMBER_LIST */
  def setShuffleMemberList(shuffle: Boolean): ClientConfig = set(SHUFFLE_MEMBER_LIST, shuffle)
  /** @see com.hazelcast.client.spi.properties.ClientProperty.MAX_CONCURRENT_INVOCATIONS */
  def setMaxConcurrentInvocations(max: Int): ClientConfig = set(MAX_CONCURRENT_INVOCATIONS, max)
  /** @see com.hazelcast.client.spi.properties.ClientProperty.BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS */
  def setBackpressureBackoffTimeout(timeout: FiniteDuration): ClientConfig = set(BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS, timeout.toMillis)

  /** @see com.hazelcast.client.spi.properties.ClientProperty.IO_INPUT_THREAD_COUNT */
  def setIOThreadCountInput(threads: Int): ClientConfig = set(IO_INPUT_THREAD_COUNT, threads)
  /** @see com.hazelcast.client.spi.properties.ClientProperty.IO_OUTPUT_THREAD_COUNT */
  def setIOThreadCountOutput(threads: Int): ClientConfig = set(IO_OUTPUT_THREAD_COUNT, threads)
  /** @see com.hazelcast.client.spi.properties.ClientProperty.IO_BALANCER_INTERVAL_SECONDS */
  def setIOBalancerInterval(interval: FiniteDuration): ClientConfig = set(IO_BALANCER_INTERVAL_SECONDS, interval.toSeconds)

    /** @see com.hazelcast.client.spi.properties.ClientProperty.INVOCATION_RETRY_PAUSE_MILLIS */
  def setInvocationRetryPause(pause: FiniteDuration): ClientConfig = set(INVOCATION_RETRY_PAUSE_MILLIS, pause.toMillis)
    /** @see com.hazelcast.client.spi.properties.ClientProperty.RESPONSE_THREAD_COUNT */
  def setResponseThreadCount(count: Int): ClientConfig = set(RESPONSE_THREAD_COUNT, count)

  /** @see com.hazelcast.client.properties.ClientProperty.HAZELCAST_CLOUD_DISCOVERY_TOKEN */
  def setHazelcastCloudDiscoveryToken(token: String): ClientConfig = set(HAZELCAST_CLOUD_DISCOVERY_TOKEN, token)

  /** @see com.hazelcast.client.properties.ClientProperty.CONCURRENT_WINDOW_MS */
  def setConcurrentWindow(window: FiniteDuration): ClientConfig = set(CONCURRENT_WINDOW_MS, window.toMillis)
}

class HzMemberProperties(conf: Config) extends HzProperties(conf) {
  import com.hazelcast.spi.properties.ClusterProperty._

  /** @see com.hazelcast.client.properties.ClientProperty.SERIALIZATION_VERSION */
  def setSerializationVersion(version: Byte): Config = set(SERIALIZATION_VERSION, version)
  /** @see com.hazelcast.client.properties.ClientProperty.QUERY_PREDICATE_PARALLEL_EVALUATION */
  def setQueryPredicateParallelEvaluation(enabled: Boolean): Config = set(QUERY_PREDICATE_PARALLEL_EVALUATION, enabled)
  /** @see com.hazelcast.client.properties.ClientProperty.QUERY_OPTIMIZER_TYPE */
  def setQueryOptimizerType(typ: QueryOptimizerFactory.Type): Config = set(QUERY_OPTIMIZER_TYPE, typ.name)
  /** @see com.hazelcast.client.properties.ClientProperty.LOCK_MAX_LEASE_TIME_SECONDS */
  def setLockMaxLeaseTime(maxLease: FiniteDuration): Config = set(LOCK_MAX_LEASE_TIME_SECONDS, maxLease.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.CLIENT_HEARTBEAT_TIMEOUT_SECONDS */
  def setClientHeartbeatTimeout(timeout: FiniteDuration): Config = set(CLIENT_HEARTBEAT_TIMEOUT_SECONDS, timeout.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.CLUSTER_SHUTDOWN_TIMEOUT_SECONDS */
  def setClusterShutdownTimeout(timeout: FiniteDuration): Config = set(CLUSTER_SHUTDOWN_TIMEOUT_SECONDS, timeout.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED */
  def setCacheInvalidationBatchEnabled(enabled: Boolean): Config = set(CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED, enabled)
  /** @see com.hazelcast.client.properties.ClientProperty.CACHE_INVALIDATION_MESSAGE_BATCH_SIZE */
  def setCacheInvalidationBatchSize(size: Int): Config = set(CACHE_INVALIDATION_MESSAGE_BATCH_SIZE, size)
  /** @see com.hazelcast.client.properties.ClientProperty.CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS */
  def setCacheInvalidationBatchFrequency(freq: FiniteDuration): Config = set(CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS, freq.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.BACKPRESSURE_ENABLED */
  def setBackpressureEnabled(enabled: Boolean): Config = set(BACKPRESSURE_ENABLED, enabled)
  /** @see com.hazelcast.client.properties.ClientProperty.BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS */
  def setBackpressureBackoffTimeout(timeout: FiniteDuration): Config = set(BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS, timeout.toMillis)
  /** @see com.hazelcast.client.properties.ClientProperty.BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION */
  def setBackpressureMaxConcurrentInvocationsPerPartition(max: Int): Config = set(BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_PARTITION, max)
  /** @see com.hazelcast.client.properties.ClientProperty.BACKPRESSURE_SYNCWINDOW */
  def setBackpressureSyncWindow(window: Int): Config = set(BACKPRESSURE_SYNCWINDOW, window)
  /** @see com.hazelcast.client.properties.ClientProperty.CLIENT_ENGINE_THREAD_COUNT */
  def setClientEngineThreadCount(size: Int): Config = set(CLIENT_ENGINE_THREAD_COUNT, size)
  /** @see com.hazelcast.client.properties.ClientProperty.CLIENT_ENGINE_QUERY_THREAD_COUNT */
  def setClientEngineQueryThreadCount(size: Int): Config = set(CLIENT_ENGINE_QUERY_THREAD_COUNT, size)
  /** @see com.hazelcast.client.properties.ClientProperty.CONNECT_ALL_WAIT_SECONDS */
  def setConnectAllWait(wait: FiniteDuration): Config = set(CONNECT_ALL_WAIT_SECONDS, wait.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.CONNECTION_MONITOR_INTERVAL */
  def setConnectionMonitorInterval(interval: FiniteDuration): Config = set(CONNECTION_MONITOR_INTERVAL, interval.toMillis)
  /** @see com.hazelcast.client.properties.ClientProperty.CONNECTION_MONITOR_MAX_FAULTS */
  def setConnectionMonitorMaxFaults(max: Int): Config = set(CONNECTION_MONITOR_MAX_FAULTS, max)
  def setLicenseKey(key: String): Config = set(ENTERPRISE_LICENSE_KEY, key)
  /** @see com.hazelcast.client.properties.ClientProperty.EVENT_QUEUE_CAPACITY */
  def setEventQueueCapacity(capacity: Int): Config = set(EVENT_QUEUE_CAPACITY, capacity)
  /** @see com.hazelcast.client.properties.ClientProperty.EVENT_QUEUE_TIMEOUT_MILLIS */
  def setEventQueueTimeout(timeout: FiniteDuration): Config = set(EVENT_QUEUE_TIMEOUT_MILLIS, timeout.toMillis)
  /** @see com.hazelcast.client.properties.ClientProperty.EVENT_SYNC_TIMEOUT_MILLIS */
  def setEventSyncTimeout(timeout: FiniteDuration): Config = set(EVENT_SYNC_TIMEOUT_MILLIS, timeout.toMillis)
  /** @see com.hazelcast.client.properties.ClientProperty.EVENT_THREAD_COUNT */
  def setEventThreadCount(threads: Int): Config = set(EVENT_THREAD_COUNT, threads)
  /** @see com.hazelcast.client.properties.ClientProperty.GRACEFUL_SHUTDOWN_MAX_WAIT */
  def setGracefulShutdownMaxWait(maxWait: FiniteDuration): Config = set(GRACEFUL_SHUTDOWN_MAX_WAIT, maxWait.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.HEALTH_MONITORING_DELAY_SECONDS */
  def setHealthMonitoringInterval(interval: FiniteDuration): Config = set(HEALTH_MONITORING_DELAY_SECONDS, interval.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.HEALTH_MONITORING_LEVEL */
  def setHealthMonitoringLevel(level: HealthMonitorLevel): Config = set(HEALTH_MONITORING_LEVEL, level.name)
  /** @see com.hazelcast.client.properties.ClientProperty.HEALTH_MONITORING_THRESHOLD_MEMORY_PERCENTAGE */
  def setHealthMonitoringMemoryThreshold(percentage: Byte): Config = set(HEALTH_MONITORING_THRESHOLD_MEMORY_PERCENTAGE, percentage)
  /** @see com.hazelcast.client.properties.ClientProperty.HEALTH_MONITORING_THRESHOLD_CPU_PERCENTAGE */
  def setHealthMonitoringCpuThreshold(percentage: Byte): Config = set(HEALTH_MONITORING_THRESHOLD_CPU_PERCENTAGE, percentage)
  /** @see com.hazelcast.client.properties.ClientProperty.HEARTBEAT_INTERVAL_SECONDS */
  def setHeartbeatInterval(interval: FiniteDuration): Config = set(HEARTBEAT_INTERVAL_SECONDS, interval.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.INITIAL_MIN_CLUSTER_SIZE */
  def setInitialMinClusterSize(minSize: Int): Config = set(INITIAL_MIN_CLUSTER_SIZE, minSize)
  /** @see com.hazelcast.client.properties.ClientProperty.INITIAL_WAIT_SECONDS */
  def setInitialWait(wait: FiniteDuration): Config = set(INITIAL_WAIT_SECONDS, wait.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.IO_BALANCER_INTERVAL_SECONDS */
  def setIOBalancerInterval(interval: FiniteDuration): Config = set(IO_BALANCER_INTERVAL_SECONDS, interval.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.IO_THREAD_COUNT */
  def setIOThreadCount(threads: Int): Config = set(IO_THREAD_COUNT, threads)
  /** @see com.hazelcast.client.properties.ClientProperty.IO_INPUT_THREAD_COUNT */
  def setIOThreadCountInput(threads: Int): Config = set(IO_INPUT_THREAD_COUNT, threads)
  /** @see com.hazelcast.client.properties.ClientProperty.IO_OUTPUT_THREAD_COUNT */
  def setIOThreadCountOutput(threads: Int): Config = set(IO_OUTPUT_THREAD_COUNT, threads)
  /** @see com.hazelcast.client.properties.ClientProperty.JCACHE_PROVIDER_TYPE */
  def setJCacheProviderType(providerType: String): Config = set(JCACHE_PROVIDER_TYPE, providerType)
  /** @see com.hazelcast.client.properties.ClientProperty.ENABLE_JMX */
  def setJmxEnabled(enabled: Boolean): Config = set(ENABLE_JMX, enabled)
  /** @see com.hazelcast.client.properties.ClientProperty.JMX_UPDATE_INTERVAL_SECONDS */
  def setJmxUpdateInterval(interval: FiniteDuration): Config = set(JMX_UPDATE_INTERVAL_SECONDS, interval.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.MAP_EXPIRY_DELAY_SECONDS */
  def setMapExpiryDelay(delay: FiniteDuration): Config = set(MAP_EXPIRY_DELAY_SECONDS, delay.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.MAP_LOAD_CHUNK_SIZE */
  def setMapLoadChunkSize(size: Int): Config = set(MAP_LOAD_CHUNK_SIZE, size)
  /** @see com.hazelcast.client.properties.ClientProperty.MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS */
  def setMapReplicaDelay(delay: FiniteDuration): Config = set(MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS, delay.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.MAP_WRITE_BEHIND_QUEUE_CAPACITY */
  def setMapWriteBehindNonCoalescingCapacity(capacity: Int): Config = set(MAP_WRITE_BEHIND_QUEUE_CAPACITY, capacity)
  /** @see com.hazelcast.client.properties.ClientProperty.MAX_JOIN_MERGE_TARGET_SECONDS */
  def setMergeTargetTimeout(timeout: FiniteDuration): Config = set(MAX_JOIN_MERGE_TARGET_SECONDS, timeout.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.MAX_JOIN_SECONDS */
  def setJoinTimeout(timeout: FiniteDuration): Config = set(MAX_JOIN_SECONDS, timeout.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.MAX_NO_HEARTBEAT_SECONDS */
  def setHeartbeatTimeout(timeout: FiniteDuration): Config = set(MAX_NO_HEARTBEAT_SECONDS, timeout.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.MAX_WAIT_SECONDS_BEFORE_JOIN */
  def setMaxWaitBeforeJoin(wait: FiniteDuration): Config = set(MAX_WAIT_SECONDS_BEFORE_JOIN, wait.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.MC_MAX_VISIBLE_SLOW_OPERATION_COUNT */
  def setManCenterMaxVisibleSlowOps(count: Int): Config = set(MC_MAX_VISIBLE_SLOW_OPERATION_COUNT, count)
  /** @see com.hazelcast.client.properties.ClientProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS */
  def setMemberListPublishInterval(interval: FiniteDuration): Config = set(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS, interval.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.MERGE_FIRST_RUN_DELAY_SECONDS */
  def setMergeRunDelay(delay: FiniteDuration): Config = set(MERGE_FIRST_RUN_DELAY_SECONDS, delay.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.MERGE_NEXT_RUN_DELAY_SECONDS */
  def setMergeRunInterval(interval: FiniteDuration): Config = set(MERGE_NEXT_RUN_DELAY_SECONDS, interval.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.OPERATION_BACKUP_TIMEOUT_MILLIS */
  def setOperationBackupTimeout(timeout: FiniteDuration): Config = set(OPERATION_BACKUP_TIMEOUT_MILLIS, timeout.toMillis)
  /** @see com.hazelcast.client.properties.ClientProperty.OPERATION_CALL_TIMEOUT_MILLIS */
  def setOperationCallTimeout(timeout: FiniteDuration): Config = set(OPERATION_CALL_TIMEOUT_MILLIS, timeout.toMillis)
  /** @see com.hazelcast.client.properties.ClientProperty.GENERIC_OPERATION_THREAD_COUNT */
  def setGenericOperationThreadCount(threads: Int): Config = set(GENERIC_OPERATION_THREAD_COUNT, threads)
  /** @see com.hazelcast.client.properties.ClientProperty.PRIORITY_GENERIC_OPERATION_THREAD_COUNT */
  def setGenericPriorityOperationThreadCount(threads: Int): Config = set(PRIORITY_GENERIC_OPERATION_THREAD_COUNT, threads)
  /** @see com.hazelcast.client.properties.ClientProperty.PARTITION_OPERATION_THREAD_COUNT */
  def setPartitionOperationThreadCount(threads: Int): Config = set(PARTITION_OPERATION_THREAD_COUNT, threads)
  /** @see com.hazelcast.client.properties.ClientProperty.PARTITION_BACKUP_SYNC_INTERVAL */
  def setPartitionBackupSyncInterval(interval: FiniteDuration): Config = set(PARTITION_BACKUP_SYNC_INTERVAL, interval.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.PARTITION_COUNT */
  def setPartitionCount(partitions: Int): Config = set(PARTITION_COUNT, partitions)
  /** @see com.hazelcast.client.properties.ClientProperty.PARTITION_MAX_PARALLEL_REPLICATIONS */
  def setPartitionMaxParallelReplications(max: Int): Config = set(PARTITION_MAX_PARALLEL_REPLICATIONS, max)
  /** @see com.hazelcast.client.properties.ClientProperty.PARTITION_MIGRATION_INTERVAL */
  def setPartitionMigrationInterval(interval: FiniteDuration): Config = set(PARTITION_MIGRATION_INTERVAL, interval.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.PARTITION_MIGRATION_TIMEOUT */
  def setPartitionMigrationTimeout(timeout: FiniteDuration): Config = set(PARTITION_MIGRATION_TIMEOUT, timeout.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.PARTITION_TABLE_SEND_INTERVAL */
  def setPartitionTablePublishInterval(interval: FiniteDuration): Config = set(PARTITION_TABLE_SEND_INTERVAL, interval.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.PARTITIONING_STRATEGY_CLASS */
  def setPartitioningStrategy(cls: Class[_ <: PartitioningStrategy[_]]): Config = set(PARTITIONING_STRATEGY_CLASS, cls.getName)
  /** @see com.hazelcast.client.properties.ClientProperty.PREFER_IPv4_STACK */
  def setPreferIPv4(prefer: Boolean): Config = set(PREFER_IPv4_STACK, prefer)
  /** @see com.hazelcast.client.properties.ClientProperty.QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK */
  def setQueryMaxLocalPartitionPreCheckLimit(limit: Int): Config = set(QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK, limit)
  /** @see com.hazelcast.client.properties.ClientProperty.QUERY_RESULT_SIZE_LIMIT */
  def setQueryResultSizeLimit(limit: Int): Config = set(QUERY_RESULT_SIZE_LIMIT, limit)
  /** @see com.hazelcast.client.properties.ClientProperty.SHUTDOWNHOOK_ENABLED */
  def setShutdownHookEnabled(enabled: Boolean): Config = set(SHUTDOWNHOOK_ENABLED, enabled)
  /** @see com.hazelcast.client.properties.ClientProperty.SLOW_OPERATION_DETECTOR_ENABLED */
  def setSlowOpsDetectorEnabled(enabled: Boolean): Config = set(SLOW_OPERATION_DETECTOR_ENABLED, enabled)
  /** @see com.hazelcast.client.properties.ClientProperty.SLOW_OPERATION_DETECTOR_LOG_PURGE_INTERVAL_SECONDS */
  def setSlowOpsDetectorLogPurgeInterval(interval: FiniteDuration): Config = set(SLOW_OPERATION_DETECTOR_LOG_PURGE_INTERVAL_SECONDS, interval.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS */
  def setSlowOpsDetectorLogRetention(retention: FiniteDuration): Config = set(SLOW_OPERATION_DETECTOR_LOG_RETENTION_SECONDS, retention.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED */
  def setSlowOpsDetectorStackTraceLoggingEnabled(enabled: Boolean): Config = set(SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED, enabled)
  /** @see com.hazelcast.client.properties.ClientProperty.SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS */
  def setSlowOpsDetectorThreshold(threshold: FiniteDuration): Config = set(SLOW_OPERATION_DETECTOR_THRESHOLD_MILLIS, threshold.toMillis)
  /** @see com.hazelcast.client.properties.ClientProperty.SOCKET_BIND_ANY */
  def setSocketBindAny(any: Boolean): Config = set(SOCKET_BIND_ANY, any)
  /** @see com.hazelcast.client.properties.ClientProperty.SOCKET_CLIENT_BIND */
  def setSocketClientBind(bind: Boolean): Config = set(SOCKET_CLIENT_BIND, bind)
  /** @see com.hazelcast.client.properties.ClientProperty.SOCKET_CLIENT_BIND_ANY */
  def setSocketClientBindAny(any: Boolean): Config = set(SOCKET_CLIENT_BIND_ANY, any)
  /** @see com.hazelcast.client.properties.ClientProperty.SOCKET_CLIENT_RECEIVE_BUFFER_SIZE */
  def setSocketClientReceiveBufferSize(size: MemorySize): Config = set(SOCKET_CLIENT_RECEIVE_BUFFER_SIZE, size.kiloBytes)
  /** @see com.hazelcast.client.properties.ClientProperty.SOCKET_CLIENT_SEND_BUFFER_SIZE */
  def setSocketClientSendBufferSize(size: MemorySize): Config = set(SOCKET_CLIENT_SEND_BUFFER_SIZE, size.kiloBytes)
  /** @see com.hazelcast.client.properties.ClientProperty.SOCKET_CONNECT_TIMEOUT_SECONDS */
  def setSocketConnectTimeout(timeout: Duration): Config = set(SOCKET_CONNECT_TIMEOUT_SECONDS, (if (timeout.isFinite) timeout.toSeconds else 0))
  /** @see com.hazelcast.client.properties.ClientProperty.SOCKET_KEEP_ALIVE */
  def setSocketKeepAlive(keepAlive: Boolean): Config = set(SOCKET_KEEP_ALIVE, keepAlive)
  /** @see com.hazelcast.client.properties.ClientProperty.SOCKET_LINGER_SECONDS */
  def setSocketLinger(linger: FiniteDuration): Config = set(SOCKET_LINGER_SECONDS, linger.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.SOCKET_NO_DELAY */
  def setSocketNoDelay(noDelay: Boolean): Config = set(SOCKET_NO_DELAY, noDelay)
  /** @see com.hazelcast.client.properties.ClientProperty.SOCKET_RECEIVE_BUFFER_SIZE */
  def setSocketReceiveBufferSize(size: MemorySize): Config = set(SOCKET_RECEIVE_BUFFER_SIZE, size.kiloBytes)
  /** @see com.hazelcast.client.properties.ClientProperty.SOCKET_SEND_BUFFER_SIZE */
  def setSocketSendBufferSize(size: MemorySize): Config = set(SOCKET_SEND_BUFFER_SIZE, size.kiloBytes)
  /** @see com.hazelcast.client.properties.ClientProperty.SOCKET_SERVER_BIND_ANY */
  def setSocketServerBindAny(any: Boolean): Config = set(SOCKET_SERVER_BIND_ANY, any)
  /** @see com.hazelcast.client.properties.ClientProperty.TCP_JOIN_PORT_TRY_COUNT */
  def setTcpJoinPortTryCount(count: Int): Config = set(TCP_JOIN_PORT_TRY_COUNT, count)
  /** @see com.hazelcast.client.properties.ClientProperty.WAIT_SECONDS_BEFORE_JOIN */
  def setWaitBeforeJoin(wait: FiniteDuration): Config = set(WAIT_SECONDS_BEFORE_JOIN, wait.toSeconds)
  /** @see com.hazelcast.client.properties.ClientProperty.PHONE_HOME_ENABLED */
  def setPhoneHomeEnabled(enabled: Boolean): Config = set(PHONE_HOME_ENABLED, enabled)

  /** @see com.hazelcast.client.properties.ClientProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED */
  def setIMapNearCacheInvalidationBatchEnabled(enabled: Boolean): Config = set(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED, enabled)
  /** @see com.hazelcast.client.properties.ClientProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE */
  def setIMapNearCacheInvalidationBatchSize(size: Int): Config = set(MAP_INVALIDATION_MESSAGE_BATCH_SIZE, size)
  /** @see com.hazelcast.client.properties.ClientProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS */
  def setIMapNearCacheInvalidationBatchFrequency(freq: FiniteDuration): Config = set(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS, freq.toSeconds)

  /** @see com.hazelcast.client.properties.ClientProperty.CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED */
  def setJCacheNearCacheInvalidationBatchEnabled(enabled: Boolean): Config = set(CACHE_INVALIDATION_MESSAGE_BATCH_ENABLED, enabled)
  /** @see com.hazelcast.client.properties.ClientProperty.CACHE_INVALIDATION_MESSAGE_BATCH_SIZE */
  def setJCacheNearCacheInvalidationBatchSize(size: Int): Config = set(CACHE_INVALIDATION_MESSAGE_BATCH_SIZE, size)
  /** @see com.hazelcast.client.properties.ClientProperty.CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS */
  def setJCacheNearCacheInvalidationBatchFrequency(freq: FiniteDuration): Config = set(CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS, freq.toSeconds)

  /** @see com.hazelcast.client.properties.ClientProperty.DISABLE_STALE_READ_ON_PARTITION_MIGRATION */
  def setStateReadOnPartitionMigrationDisabled(disabled: Boolean): Config = set(DISABLE_STALE_READ_ON_PARTITION_MIGRATION, disabled)

  /** @see com.hazelcast.client.properties.ClientProperty.INIT_CLUSTER_VERSION */
  def setInitialClusterVersion(v: String): Config = set(INIT_CLUSTER_VERSION, v)

  /** @see com.hazelcast.client.properties.ClientProperty.BIND_SPOOFING_CHECKS */
  def setTCPBindSpoofingChecks(enabled: Boolean): Config = set(BIND_SPOOFING_CHECKS, enabled)

  /** @see com.hazelcast.client.properties.ClientProperty.SEARCH_DYNAMIC_CONFIG_FIRST */
  def setSearchDynamicConfigFirst(enabled: Boolean): Config = set(SEARCH_DYNAMIC_CONFIG_FIRST, enabled)
}
