package com.hazelcast.Scala

import scala.concurrent.{ ExecutionContext, Future, Promise }

import com.hazelcast.core.{
  LifecycleEvent, DistributedObjectEvent,
  LifecycleListener, DistributedObjectListener
}
import com.hazelcast.client.{ ClientListener, Client }
import com.hazelcast.cluster.{
  InitialMembershipEvent, MembershipEvent, InitialMembershipListener
}
import com.hazelcast.partition.{
  MigrationListener
}
import com.hazelcast.map.MapPartitionLostEvent
import com.hazelcast.map.listener.MapPartitionLostListener
import com.hazelcast.partition.PartitionLostEvent
import com.hazelcast.partition.PartitionLostListener
import com.hazelcast.partition.MigrationState
import com.hazelcast.partition.ReplicaMigrationEvent

private[Scala] trait EventSubscription {
  type ESR
  type MER

  def onLifecycleStateChange(runOn: ExecutionContext = null)(listener: PartialFunction[LifecycleEvent.LifecycleState, Unit]): ESR
  def onDistributedObjectEvent(runOn: ExecutionContext = null)(listener: PartialFunction[DistributedObjectChange, Unit]): ESR
  def onPartitionLost(runOn: ExecutionContext = null)(listener: PartitionLostEvent => Unit): ESR
  def onMigration(runOn: ExecutionContext = null)(listener: PartialFunction[MigrationEvent, Unit]): ESR
  def onMemberChange(runOn: ExecutionContext = null)(listener: PartialFunction[MemberEvent, Unit]): MER
}

private[Scala] trait MemberEventSubscription extends EventSubscription {

  def onClient(runOn: ExecutionContext = null)(listener: PartialFunction[ClientEvent, Unit]): ESR

}

private[Scala] trait MapEventSubscription {
  type MSR

  def onMapEvents(localOnly: Boolean = false, runOn: ExecutionContext = null)(pf: PartialFunction[MapEvent, Unit]): MSR
  def onPartitionLost(runOn: ExecutionContext = null)(listener: PartialFunction[PartitionLost, Unit]): MSR
}

private[Scala] trait MapEntryEventSubscription[K, V] {
  type MSR

  def onKeyEvents(localOnly: Boolean = false, runOn: ExecutionContext = null)(pf: PartialFunction[KeyEvent[K], Unit]): MSR
  def onEntryEvents(localOnly: Boolean = false, runOn: ExecutionContext = null)(pf: PartialFunction[EntryEvent[K, V], Unit]): MSR

  def onKeyEvents(cb: OnKeyEvent[K], localOnly: Boolean): MSR
  final def onKeyEvents(cb: OnKeyEvent[K]): MSR = onKeyEvents(cb, false)
  def onEntryEvents(cb: OnEntryEvent[K, V], localOnly: Boolean): MSR
  final def onEntryEvents(cb: OnEntryEvent[K, V]): MSR = onEntryEvents(cb, false)

}

private[Scala] object EventSubscription {
  def asLifecycleListener(listener: PartialFunction[LifecycleEvent.LifecycleState, Unit], ec: Option[ExecutionContext]) =
    new PfProxy(listener, ec) with LifecycleListener {
      def stateChanged(evt: LifecycleEvent): Unit = invokeWith(evt.getState)
    }

  def asDistributedObjectListener(listener: PartialFunction[DistributedObjectChange, Unit], ec: Option[ExecutionContext]) =
    new PfProxy(listener, ec) with DistributedObjectListener {
      def distributedObjectCreated(evt: DistributedObjectEvent) = hear(evt)
      def distributedObjectDestroyed(evt: DistributedObjectEvent) = hear(evt)
      @inline def hear(evt: DistributedObjectEvent) = {
        import DistributedObjectEvent.EventType._
        val event: DistributedObjectChange = evt.getEventType match {
          case CREATED => DistributedObjectCreated(evt.getObjectName.toString, evt.getDistributedObject)
          case DESTROYED => DistributedObjectDestroyed(evt.getObjectName.toString, evt.getServiceName)
        }
        invokeWith(event)
      }

    }

  def asPartitionLostListener(listener: PartitionLostEvent => Unit, ec: Option[ExecutionContext]) = {
    val pf: PartialFunction[PartitionLostEvent, Unit] = { case evt => listener(evt) }
    new PfProxy[PartitionLostEvent](pf, ec) with PartitionLostListener {
      def partitionLost(evt: PartitionLostEvent): Unit = invokeWith(evt)
    }
  }

  def asMigrationListener(listener: PartialFunction[MigrationEvent, Unit], ec: Option[ExecutionContext]) =
    new PfProxy(listener, ec) with MigrationListener {
      def migrationStarted(state: MigrationState) = invokeWith(MigrationStarted(state))
      def migrationFinished(state: MigrationState) = invokeWith(MigrationFinished(state))
      def replicaMigrationCompleted(evt: ReplicaMigrationEvent) = invokeWith(ReplicaMigrationCompleted(evt))
      def replicaMigrationFailed(evt: ReplicaMigrationEvent) = invokeWith(ReplicaMigrationFailed(evt))

    }

  def asMembershipListener(
      listener: PartialFunction[MemberEvent, Unit],
      ec: Option[ExecutionContext])
      : (Future[InitialMembershipEvent], InitialMembershipListener) = {

    import MembershipEvent._
    import collection.JavaConverters._

    val promise = Promise[InitialMembershipEvent]()
    promise.future -> new PfProxy(listener, ec) with InitialMembershipListener {
      def init(evt: InitialMembershipEvent) = promise success evt
      def memberAdded(evt: MembershipEvent) = hear(evt)
      def memberRemoved(evt: MembershipEvent) = hear(evt)
      private def hear(evt: MembershipEvent) = {
        val event: MemberEvent = evt match {
          case evt: MembershipEvent if evt.getEventType == MEMBER_ADDED =>
            MemberAdded(evt.getMember, evt.getMembers.asScala)(evt.getCluster)
          case evt: MembershipEvent if evt.getEventType == MEMBER_REMOVED =>
            MemberRemoved(evt.getMember, evt.getMembers.asScala)(evt.getCluster)
        }
        invokeWith(event)
      }
    }
  }

  def asPartitionLostListener(listener: PartialFunction[PartitionLost, Unit], ec: Option[ExecutionContext]): MapPartitionLostListener =
    new PfProxy(listener, ec) with MapPartitionLostListener {
      def partitionLost(evt: MapPartitionLostEvent) = invokeWith(new PartitionLost(evt.getMember, evt.getPartitionId)(evt))
    }

  def asClientListener(listener: PartialFunction[ClientEvent, Unit], ec: Option[ExecutionContext]) =
    new PfProxy(listener, ec) with ClientListener {
      def clientConnected(client: Client) = invokeWith(new ClientConnected(client))
      def clientDisconnected(client: Client) = invokeWith(new ClientDisconnected(client))
    }
}
