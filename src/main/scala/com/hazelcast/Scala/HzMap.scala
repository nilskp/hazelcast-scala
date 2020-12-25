package com.hazelcast.Scala

import java.util.Comparator
import java.util.Map.Entry

import scala.collection.JavaConverters._
import scala.collection.mutable.{ Map => mMap }
import scala.collection.{ Set => cSet }
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Try

import com.hazelcast.Scala.dds.DDS
import com.hazelcast.Scala.dds.MapDDS
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.HazelcastInstanceAware
import com.hazelcast.core.ReadOnly
import com.hazelcast.map.IMap
import com.hazelcast.query.impl.predicates.PagingPredicateImpl
import com.hazelcast.query.Predicate
import com.hazelcast.query.PredicateBuilder
import com.hazelcast.spi.impl.AbstractDistributedObject
import com.hazelcast.map.impl.recordstore.RecordStore
import com.hazelcast.internal.serialization.Data
import scala.collection.parallel.ParIterable
import com.hazelcast.map.impl.MapServiceContext
import com.hazelcast.map.impl.record.Record
import com.hazelcast.map.EntryProcessor

final class HzMap[K, V](protected val imap: IMap[K, V])
  extends KeyedIMapDeltaUpdates[K, V]
  with MapEventSubscription {

  private[Scala] def getHZ: HazelcastInstance = imap match {
    case ado: AbstractDistributedObject[_] => ado.getNodeEngine.getHazelcastInstance
    case _ => getClientHzProxy(imap) getOrElse sys.error(s"Cannot get HazelcastInstance from ${imap.getClass}")
  }

  private[Scala] def getPartitionId(key: K): Int = getHZ.getPartitionService.getPartition(key).getPartitionId

  private[Scala] def getFastIfLocal(keysByPartition: ParIterable[(Int, cSet[K])]): ParIterable[Entry[K, V]] = {
    HzMap.mapServiceContext(imap).map { implicit ctx =>
      keysByPartition.flatMap {
        case (partitionId, keys) =>
          ctx.getExistingRecordStore(partitionId, imap.getName) match {
            case recStore: RecordStore[_] =>
              keys.iterator.map { key =>
                val value = getValueOrNull(key, recStore) match {
                  case null => blocking(imap get key)
                  case value => value
                }
                new ImmutableEntry(key, value)
              }.filter(_.value != null)
            case _ => blocking(imap.getAll(keys.asJava)).entrySet.asScala
          }
      }
    } getOrElse {
      keysByPartition.flatMap {
        case (_, keys) => blocking(imap.getAll(keys.asJava)).entrySet.asScala
      }
    }
  }

  private def getValueOrNull(key: K, store: RecordStore[_ <: Record[_]])(implicit ctx: MapServiceContext): V = {
    store.getRecordOrNull(ctx.toData(key)) match {
      case null => null.asInstanceOf[V]
      case record => record.getValue match {
        case data: Data => ctx.toObject(data).asInstanceOf[V]
        case value => value.asInstanceOf[V]
      }
    }
  }

  private[Scala] def getFastIfLocal(key: K, partitionId: Int = -1): V = {
    HzMap.mapServiceContext(imap).map { implicit ctx =>
      val parId = if (partitionId >= 0) partitionId else getPartitionId(key)
      val valueOrNull = ctx.getExistingRecordStore(parId, imap.getName) match {
        case recStore: RecordStore[_] => getValueOrNull(key, recStore)
        case _ => null.asInstanceOf[V]
      }
      valueOrNull match {
        case null => imap.get(key)
        case value => value
      }
    } getOrElse {
      imap.get(key)
    }

  }

  def async: AsyncMap[K, V] = new AsyncMap(imap)

  import ExecutionContext.parasitic

  /**
   * NOTE: This method is generally slower than `get`
   * and also will not be part of `get` statistics.
   * It is meant for very large objects where only a
   * subset of the data is needed, thus limiting
   * unnecessary network traffic.
   */
  def getAs[R](key: K)(map: V => R): Option[R] =
    async.getAs(key)(map)(parasitic).await

  def getAs[C, R](getCtx: HazelcastInstance => C, key: K)(mf: (C, V) => R): Option[R] =
    async.getAs(getCtx, key)(mf)(parasitic).await

  def getAll(keys: cSet[K]): mMap[K, V] =
    if (keys.isEmpty) mMap.empty
    else imap.getAll(keys.asJava).asScala

  def getAllAs[R](keys: cSet[K])(mf: V => R): mMap[K, R] =
    if (keys.isEmpty) mMap.empty
    else {
      val ep = new HzMap.GetAllAsEP[K, V, R](mf)
      imap
        .executeOnKeys(keys.asJava, ep)
        .asScala
    }

  def query[T](pred: Predicate[K, V])(mf: V => T): mMap[K, T] = {
    val ep = new HzMap.QueryEP[K, V, T](mf)
    imap
      .executeOnEntries(ep, pred)
      .asScala
      // .asInstanceOf[mMap[K, T]]
  }
  def query[C, T](ctx: HazelcastInstance => C, pred: Predicate[K, V])(mf: (C, K, V) => T): mMap[K, T] = {
    val ep = new HzMap.ContextQueryEP[C, K, V, T](ctx, mf)
    imap
      .executeOnEntries(ep, pred)
      .asScala
      // .asInstanceOf[mMap[K, T]]
  }

  def foreach[C](ctx: HazelcastInstance => C, pred: Predicate[K, V] = null)(thunk: (C, K, V) => Unit): Unit = {
    val ep = new HzMap.ForEachEP[K, V, C](ctx, thunk)
    pred match {
      case null => imap.executeOnEntries(ep)
      case pred => imap.executeOnEntries(ep, pred)
    }
  }

  private def updateValues(predicate: Option[Predicate[K, V]], update: V => V, returnValue: V => Object): mMap[K, V] = {
    val ep = new HzMap.ValueUpdaterEP[K, V](update, returnValue)
    val map = predicate match {
      case Some(predicate) => imap.executeOnEntries(ep, predicate)
      case None => imap.executeOnEntries(ep)
    }
    map.asScala.asInstanceOf[mMap[K, V]]
  }

  def updateAll(predicate: Predicate[K, V] = null)(updateIfPresent: V => V): Unit = {
    updateValues(Option(predicate), updateIfPresent, _ => null)
  }
  def updateAndGetAll(predicate: Predicate[K, V])(updateIfPresent: V => V): mMap[K, V] = {
    updateValues(Option(predicate), updateIfPresent, _.asInstanceOf[Object])
  }

  def set(key: K, value: V, ttl: Duration): Unit = {
    if (ttl.isFinite && ttl.length > 0) {
      imap.set(key, value, ttl.length, ttl.unit)
    } else {
      imap.set(key, value)
    }
  }

  def put(key: K, value: V, ttl: Duration): Option[V] = {
    if (ttl.isFinite && ttl.length > 0) {
      Option(imap.put(key, value, ttl.length, ttl.unit))
    } else {
      Option(imap.put(key, value))
    }
  }
  def setTransient(key: K, value: V, ttl: Duration = Duration.Inf): Unit = {
    if (ttl.isFinite) {
      imap.putTransient(key, value, ttl.length, ttl.unit)
    } else {
      imap.putTransient(key, value, 0, MILLISECONDS)
    }
  }
  def putIfAbsent(key: K, value: V, ttl: Duration): Option[V] = {
    if (ttl.isFinite && ttl.length > 0) {
      Option(imap.putIfAbsent(key, value, ttl.length, ttl.unit))
    } else {
      Option(imap.putIfAbsent(key, value))
    }
  }

  def setIfAbsent(key: K, value: V, ttl: Duration = Duration.Inf): Boolean =
    async.setIfAbsent(key, value, ttl)(parasitic).await

  def execute[R](filter: EntryFilter[K, V])(thunk: Entry[K, filter.EV] => R): filter.M[R] = {
      def ep: EntryProcessor[K, filter.EV, R] = new HzMap.ExecuteEP(thunk)
    filter match {
      case ok @ OnKey(key) =>
        val onKeyThunk = thunk.asInstanceOf[Entry[K, ok.EV] => R]
        imap
          .executeOnKey(key, new HzMap.ExecuteOptEP(onKeyThunk))
          .asInstanceOf[filter.M[R]]
      case OnEntries(null) =>
        imap
          .executeOnEntries(ep)
          .asScala
          // .asInstanceOf[filter.M[R]]
      case OnEntries(predicate) =>
        imap
          .executeOnEntries(ep, predicate)
          .asScala
          // .asInstanceOf[filter.M[R]]
      case OnKeys(keys) => {
        if (keys.isEmpty) java.util.Collections.emptyMap[K, R]()
        else imap.executeOnKeys(keys.asJava, ep)
      }.asScala
      // .asInstanceOf[filter.M[R]]
      case OnValues(include) =>
        imap.executeOnEntries(ep, new ValuePredicate(include))
          .asScala
          // .asInstanceOf[filter.M[R]]
    }
  }

  type MSR = ListenerRegistration
  def onMapEvents(localOnly: Boolean, runOn: ExecutionContext)(pf: PartialFunction[MapEvent, Unit]): MSR = {
    val listener: com.hazelcast.map.listener.MapListener = new MapListener(pf, Option(runOn))
    val regId =
      if (localOnly) imap.addLocalEntryListener(listener)
      else imap.addEntryListener(listener, /* includeValue = */ false)
    new ListenerRegistration {
      def cancel(): Boolean = imap.removeEntryListener(regId)
    }
  }
  def onPartitionLost(runOn: ExecutionContext)(listener: PartialFunction[PartitionLost, Unit]): MSR = {
    val regId = imap addPartitionLostListener EventSubscription.asPartitionLostListener(listener, Option(runOn))
    new ListenerRegistration {
      def cancel() = imap removePartitionLostListener regId
    }
  }

  def filter(pred: PredicateBuilder): DDS[Entry[K, V]] = new MapDDS(imap, pred.asInstanceOf[Predicate[K, V]])
  def filter(pred: Predicate[K, V]): DDS[Entry[K, V]] = new MapDDS(imap, pred)

  // TODO: Perhaps a macro could turn this into an IndexAwarePredicate?
  def filter(f: (K, V) => Boolean): DDS[Entry[K, V]] = new MapDDS(imap, new EntryPredicate(f))

  def values[O: Ordering](range: Range, pred: Predicate[K, V] = null)(sortBy: Entry[K, V] => O, reverse: Boolean = false): Iterable[V] = {
    val pageSize = range.length
    val pageIdx = range.min / pageSize
    val dropValues = range.min % pageSize
    val comparator = new Comparator[Entry[K, V]] with Serializable {
      private[this] val ordering = implicitly[Ordering[O]] match {
        case comp if reverse => comp.reverse
        case comp => comp
      }
      def compare(a: Entry[K, V], b: Entry[K, V]): Int = ordering.compare(sortBy(a), sortBy(b))
    }
    val pp = new PagingPredicateImpl(pred, comparator, pageSize)
    pp.setPage(pageIdx)
    val result = imap.values(pp).asScala
    if (dropValues == 0) result
    else result.drop(dropValues)
  }
  def entries[O: Ordering](range: Range, pred: Predicate[K, V] = null)(sortBy: Entry[K, V] => O, reverse: Boolean = false): Iterable[Entry[K, V]] = {
    val pageSize = range.length
    val pageIdx = range.min / pageSize
    val dropEntries = range.min % pageSize
    val comparator = new Comparator[Entry[K, V]] with Serializable {
      private[this] val ordering = implicitly[Ordering[O]] match {
        case comp if reverse => comp.reverse
        case comp => comp
      }
      def compare(a: Entry[K, V], b: Entry[K, V]): Int =
        ordering.compare(sortBy(a), sortBy(b))
    }
    val pp = new PagingPredicateImpl(pred, comparator, pageSize)
    pp.setPage(pageIdx)
    val result = imap.entrySet(pp).iterator.asScala.toIterable
    if (dropEntries == 0) result
    else result.drop(dropEntries)
  }
  def keys[O: Ordering](range: Range, localOnly: Boolean = false, pred: Predicate[K, V] = null)(sortBy: Entry[K, V] => O, reverse: Boolean = false): Iterable[K] = {
    val pageSize = range.length
    val pageIdx = range.min / pageSize
    val dropEntries = range.min % pageSize
    val comparator = new Comparator[Entry[K, V]] with Serializable {
      private[this] val ordering = implicitly[Ordering[O]] match {
        case comp if reverse => comp.reverse
        case comp => comp
      }
      def compare(a: Entry[K, V], b: Entry[K, V]): Int =
        ordering.compare(sortBy(a), sortBy(b))
    }
    val pp = new PagingPredicateImpl(pred, comparator, pageSize)
    pp.setPage(pageIdx)
    val result = (if (localOnly) imap.localKeySet(pp) else imap.keySet(pp)).iterator.asScala.toIterable
    if (dropEntries == 0) result
    else result.drop(dropEntries)
  }
}

private[Scala] object HzMap {

  import com.hazelcast.map.impl.proxy.MapProxyImpl
  import com.hazelcast.map.impl.MapServiceContext

  private[this] val MapServiceCtxField = Try {
    val field = classOf[MapProxyImpl[_, _]].getSuperclass.getDeclaredField("mapServiceContext")
    field.setAccessible(true)
    field
  }
  def mapServiceContext(imap: IMap[_, _]): Try[MapServiceContext] =
    MapServiceCtxField
      .flatMap { field =>
        Try(field.get(imap).asInstanceOf[MapServiceContext])
      }

  final class GetAllAsEP[K, V, T](_mf: V => T)
  extends EntryProcessor[K, V, T]
  with ReadOnly {
    def mf = _mf
    def process(entry: Entry[K, V]): T =
      entry.value match {
        case null => null.asInstanceOf[T]
        case value => _mf(value)
      }

  }
  final class QueryEP[K, V, T](_mf: V => T)
  extends EntryProcessor[K, V, T]
  with ReadOnly {
    def mf = _mf
    def process(entry: Entry[K, V]): T =
      _mf(entry.value)
  }
  final class ContextQueryEP[C, K, V, T](val getCtx: HazelcastInstance => C, _mf: (C, K, V) => T)
  extends EntryProcessor[K, V, T]
  with HazelcastInstanceAware
  with ReadOnly {
    @transient private[this] var ctx: C = _
    def setHazelcastInstance(hz: HazelcastInstance): Unit = {
      this.ctx = getCtx(hz)
    }
    def mf = _mf
    def process(entry: Entry[K, V]): T =
      _mf(ctx, entry.key, entry.value)
  }
  final class ForEachEP[K, V, C](val getCtx: HazelcastInstance => C, _thunk: (C, K, V) => Unit)
  extends EntryProcessor[K, V, Null]
  with HazelcastInstanceAware
  with ReadOnly {
    def thunk = _thunk
    @transient private[this] var ctx: C = _
    def setHazelcastInstance(hz: HazelcastInstance) = ctx = getCtx(hz)
    def process(entry: Entry[K, V]): Null = {
      _thunk(ctx, entry.key, entry.value)
      null
    }
  }
  final class ValueUpdaterEP[K, V](_update: V => V, _returnValue: V => Object)
  extends EntryProcessor[K, V, Object] {
    def update = _update
    def returnValue = _returnValue
    def process(entry: Entry[K, V]): Object = {
      entry.value = _update(entry.value)
      _returnValue(entry.value)
    }
  }
  final class ExecuteEP[K, V, R](_thunk: Entry[K, V] => R)
  extends EntryProcessor[K, V, R] {
    def thunk = _thunk
    def process(entry: Entry[K, V]): R = _thunk(entry) match {
      case null | _: Unit => null.asInstanceOf[R]
      case value => value
    }
  }
  final class ExecuteOptEP[K, V, R](_thunk: Entry[K, Option[V]] => R)
  extends EntryProcessor[K, V, R] {
    private class OptEntry(org: Entry[K, V]) extends Entry[K, Option[V]] {
      def getKey() = org.getKey
      def getValue() = Option(org.getValue)
      def setValue(opt: Option[V]): Option[V] = Option(org.setValue(opt getOrElse null.asInstanceOf[V]))
    }
    def thunk = _thunk
    def process(entry: Entry[K, V]): R = _thunk(new OptEntry(entry)) match {
      case null | _: Unit => null.asInstanceOf[R]
      case value => value
    }
  }
}
