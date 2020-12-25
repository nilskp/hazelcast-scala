package com.hazelcast.Scala.dds

// import language.existentials

import collection.{ Map => cMap }
import collection.parallel.CollectionConverters._
import concurrent._

import com.hazelcast.core._
import com.hazelcast.query._
import com.hazelcast.cluster.Member
import com.hazelcast.Scala._
import com.hazelcast.Scala.aggr._

import java.util.Map.Entry
import java.util.UUID

private[Scala] class AggrMapDDS[K, V, E](val dds: MapDDS[K, V, E], sorted: Option[Sorted[E]] = None) extends AggrDDS[E] {

  def this(dds: MapDDS[K, V, E], sorted: Sorted[E]) = this(dds, Option(sorted))

  final def submit[R](
      aggregator: Aggregator[E, R],
      esOrNull: IExecutorService,
      tsOrNull: UserContext.Key[collection.parallel.TaskSupport])(
      implicit
      ec: ExecutionContext)
      : Future[R] = {

    val hz = dds.imap.getHZ
    val keysByMember = dds.keySet.map(hz.groupByMember)
    val es = if (esOrNull == null) hz.queryPool else esOrNull
    val ts = Option(tsOrNull)

    sorted match {
      case None =>
        AggrMapDDS.aggregate[K, V, E, R, aggregator.W](dds.imap.getName, keysByMember, dds.predicate, dds.pipe, es, aggregator, ts)
      case Some(sorted) =>
        aggregator match {
          case _: Values.Complete[_] =>
            val fetch = aggr.Values(sorted)
            AggrMapDDS.aggregate(dds.imap.getName, keysByMember, dds.predicate, dds.pipe, es, fetch, ts)
          case _ =>
            val adapter = aggr.Values.Adapter(aggregator, sorted)
            AggrMapDDS.aggregate[K, V, E, R, adapter.W](dds.imap.getName, keysByMember, dds.predicate, dds.pipe, es, adapter, ts)
        }
    }
  }

}

private[Scala] class AggrGroupMapDDS[G, E](dds: MapDDS[_, _, (G, E)]) extends AggrGroupDDS[G, E] {
  def submitGrouped[AR, GR](
    aggr: Aggregator.Grouped[G, E, AR, GR],
    es: IExecutorService,
    ts: UserContext.Key[collection.parallel.TaskSupport])(implicit ec: ExecutionContext): Future[cMap[G, GR]] =
    dds.submit(aggr, es, ts)
}

private[Scala] class OrderingMapDDS[K, V, O: Ordering](
  dds: MapDDS[K, V, O], sorted: Option[Sorted[O]] = None)
    extends AggrMapDDS(dds, sorted) with OrderingDDS[O] {
  def this(dds: MapDDS[K, V, O], sorted: Sorted[O]) = this(dds, Option(sorted))
  final protected def ord = implicitly[Ordering[O]]
}
private[Scala] class OrderingGroupMapDDS[G, O: Ordering](dds: MapDDS[_, _, (G, O)])
    extends AggrGroupMapDDS(dds) with OrderingGroupDDS[G, O] {
  final protected def ord = implicitly[Ordering[O]]
}

private[Scala] class NumericMapDDS[K, V, N: Numeric](
  dds: MapDDS[K, V, N], sorted: Option[Sorted[N]] = None)
    extends OrderingMapDDS(dds, sorted) with NumericDDS[N] {
  def this(dds: MapDDS[K, V, N], sorted: Sorted[N]) = this(dds, Option(sorted))
  final protected def num = implicitly[Numeric[N]]
}
private[Scala] class NumericGroupMapDDS[G, N: Numeric](dds: MapDDS[_, _, (G, N)])
    extends OrderingGroupMapDDS(dds) with NumericGroupDDS[G, N] {
  final protected def num = implicitly[Numeric[N]]
}

private[Scala] final class AggrMapDDSTask[K, V, E, AW](
  val aggr: Aggregator[E, _] { type W = AW },
  val taskSupport: Option[UserContext.Key[collection.parallel.TaskSupport]],
  val mapName: String,
  val keysByMemberId: Map[UUID, collection.Set[K]],
  val predicate: Option[Predicate[K, V]],
  val pipe: Pipe[E])
extends (HazelcastInstance => AW)
with Serializable {

  import scala.jdk.CollectionConverters._

  def apply(hz: HazelcastInstance): AW = {
    aggr match {
      case aggr: HazelcastInstanceAware => aggr setHazelcastInstance hz
      case _ => // Ignore
    }
    val folded = processLocalData(hz)
    aggr.remoteFinalize(folded)
  }
  private def processLocalData(hz: HazelcastInstance): aggr.Q = {
    val imap = hz.getMap[K, V](mapName)
    val (localKeys, includeEntry) =
      keysByMemberId.get(hz.getCluster.getLocalMember.getUuid) match {
        case None =>
          assert(keysByMemberId.isEmpty) // If keys are known, this code should not be running on this member
          predicate
            .map(imap.localKeySet(_))
            .getOrElse(imap.localKeySet)
            .asScala -> Predicates.alwaysTrue[K, V]
        case Some(keys) =>
          keys -> predicate.getOrElse(Predicates.alwaysTrue[K, V])
      }
    if (localKeys.isEmpty) aggr.remoteInit
    else {

      type Q = aggr.Q
      val remoteFold = aggr.remoteFold _
      val entryFold = pipe.prepare[Q](hz)
      val seqop = (acc: Q, entry: Entry[K, V]) => {
        if (includeEntry(entry)) {
          entryFold.foldEntry(acc, entry)(remoteFold)
        } else acc
      }
      val keysByPartition = hz.groupByPartitionId(localKeys).toIterable.par
      taskSupport.foreach { taskSupport =>
        keysByPartition.tasksupport = hz.userCtx(taskSupport)
      }
      val entries = imap.getFastIfLocal(keysByPartition)
      entries.aggregate(aggr.remoteInit)(seqop, aggr.remoteCombine)
    }
  }
}

private object AggrMapDDS {

  type OptionalTaskSupport = Option[UserContext.Key[collection.parallel.TaskSupport]]

  private def aggregate[K, V, E, R, AW](
    mapName: String,
    keysByMember: Option[Map[Member, collection.Set[K]]],
    predicate: Option[Predicate[K, V]],
    pipe: Option[Pipe[E]],
    es: IExecutorService,
    aggr: Aggregator[E, R] { type W = AW },
    taskSupport: OptionalTaskSupport)(implicit ec: ExecutionContext): Future[R] = {

    val (keysByMemberId, submitTo) = keysByMember match {
      case None => Map.empty[UUID, collection.Set[K]] -> ToAll
      case Some(keysByMember) =>
        val keysByMemberId = keysByMember.map {
          case (member, keys) => member.getUuid -> keys
        }
        keysByMemberId -> ToMembers(keysByMember.keys)
    }
    val values = submitFold(es, submitTo, mapName, keysByMemberId, predicate, pipe getOrElse PassThroughPipe[E], aggr, taskSupport)
    val reduced = Future.reduceLeft(values.toSeq)(aggr.localCombine _)
    reduced.map(aggr.localFinalize(_))(SameThread)
  }
  private def submitFold[K, V, E](
    es: IExecutorService,
    submitTo: MultipleMembers,
    mapName: String,
    keysByMemberId: Map[UUID, collection.Set[K]],
    predicate: Option[Predicate[K, V]],
    pipe: Pipe[E],
    aggr: Aggregator[E, _],
    taskSupport: OptionalTaskSupport): Iterable[Future[aggr.W]] = {

    val remoteTask = new AggrMapDDSTask[K, V, E, aggr.W](aggr, taskSupport, mapName, keysByMemberId, predicate, pipe)
    es.submit(submitTo)(remoteTask).values
  }

}
