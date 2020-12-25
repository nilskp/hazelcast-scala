package com.hazelcast.Scala

import com.hazelcast.partition.{ MigrationState, ReplicaMigrationEvent }

sealed abstract class MigrationEvent

case class MigrationStarted(state: MigrationState) extends MigrationEvent
case class MigrationFinished(state: MigrationState) extends MigrationEvent
case class ReplicaMigrationCompleted(evt: ReplicaMigrationEvent) extends MigrationEvent
case class ReplicaMigrationFailed(evt: ReplicaMigrationEvent) extends MigrationEvent
