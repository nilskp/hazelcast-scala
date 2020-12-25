package com.hazelcast.Scala

import java.util.Map.Entry
import java.util.concurrent.TimeUnit

import scala.beans.BeanProperty
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.Duration

import com.hazelcast.core._
import com.hazelcast.map.IMap

final class AsyncMap[K, V] private[Scala] (protected val imap: IMap[K, V])
extends KeyedIMapAsyncDeltaUpdates[K, V] {

  def get(key: K)(implicit ec: ExecutionContext): Future[Option[V]] =
    imap.getAsync(key).asScalaOpt

  def getAll(keys: Set[K])(implicit ec: ExecutionContext): Future[Map[K, V]] = {
    val fResults = keys.iterator.map { key =>
      this.get(key).map(_.map(key -> _))
    }
    Future.sequence(fResults).map(_.flatten.toMap)
  }
  def getAllAs[R](keys: Set[K])(mf: V => R)(implicit ec: ExecutionContext): Future[Map[K, R]] = {
    val fResults = keys.iterator.map { key =>
      this.getAs(key)(mf).map(_.map(key -> _))
    }
    Future.sequence(fResults).map(_.flatten.toMap)
  }

  def put(key: K, value: V, ttl: Duration = Duration.Inf)(implicit ec: ExecutionContext): Future[Option[V]] =
    if (ttl.isFinite && ttl.length > 0) {
      imap.putAsync(key, value, ttl.length, ttl.unit).asScalaOpt
    } else {
      imap.putAsync(key, value).asScalaOpt
    }

  def putIfAbsent(
      key: K, value: V, ttl: Duration = Duration.Inf)(
      implicit
      ec: ExecutionContext): Future[Option[V]] = {
    val ep =
      if (ttl.isFinite && ttl.length > 0) {
        new AsyncMap.TTLPutIfAbsentEP[K, V](imap.getName, value, ttl.length, ttl.unit)
      } else {
        new AsyncMap.PutIfAbsentEP[K, V](value)
      }
    imap
      .submitToKey(key, ep)
      .asScalaOpt
  }

  def setIfAbsent(
      key: K, value: V, ttl: Duration = Duration.Inf)(
      implicit ec: ExecutionContext): Future[Boolean] = {
    val ep =
      if (ttl.isFinite && ttl.length > 0) {
        new AsyncMap.TTLSetIfAbsentEP[K, V](imap.getName, value, ttl.length, ttl.unit)
      } else {
        new AsyncMap.SetIfAbsentEP[K, V](value)
      }
    imap
      .submitToKey(key, ep)
      .asScala
  }
  implicit private[this] val any2unit = (any: Any) => ()
  def set(
      key: K, value: V, ttl: Duration = Duration.Inf)(
      implicit
      ec: ExecutionContext)
      : Future[Unit] = {
    if (ttl.isFinite && ttl.length > 0) {
      imap.setAsync(key, value, ttl.length, ttl.unit).asScala
    } else {
      imap.setAsync(key, value).asScala
    }
  }

  def remove(key: K)(implicit ec: ExecutionContext): Future[Option[V]] =
    imap.removeAsync(key).asScalaOpt

  def getAs[R](
      key: K)(
      map: V => R)(
      implicit
      ec: ExecutionContext)
      : Future[Option[R]] = {
    val ep = new AsyncMap.GetAsEP[K, V, R](map)
    imap
      .submitToKey(key, ep)
      .asScalaOpt
  }
  def getAs[C, R](
      getCtx: HazelcastInstance => C,
      key: K)(
      mf: (C, V) => R)(
      implicit
      ec: ExecutionContext)
      : Future[Option[R]] = {
    val ep = new AsyncMap.ContextGetAsEP[C, K, V, R](getCtx, mf)
    imap
      .submitToKey(key, ep)
      .asScalaOpt
  }

}

private[Scala] object AsyncMap {
  final class GetAsEP[K, V, R](val mf: V => R)
      extends SingleEntryCallbackReader[K, V, R] {
    def onEntry(key: K, value: V): R = {
      value match {
        case null => null.asInstanceOf[R]
        case value => mf(value)
      }
    }
  }
  final class ContextGetAsEP[C, K, V, R](val getCtx: HazelcastInstance => C, val mf: (C, V) => R)
      extends SingleEntryCallbackReader[K, V, R]
      with HazelcastInstanceAware {
    @BeanProperty @transient
    var hazelcastInstance: HazelcastInstance = _
    def onEntry(key: K, value: V): R = {
      value match {
        case null => null.asInstanceOf[R]
        case value =>
          val ctx = getCtx(hazelcastInstance)
          mf(ctx, value)
      }
    }
  }
  final class TTLPutIfAbsentEP[K, V](val mapName: String, val putIfAbsent: V, val ttl: Long, val unit: TimeUnit)
      extends SingleEntryCallbackReader[K, V, V]
      with HazelcastInstanceAware {
    @BeanProperty @transient
    var hazelcastInstance: HazelcastInstance = _
    def onEntry(key: K, existing: V): V = {
      if (existing == null) {
        val imap = hazelcastInstance.getMap[K, V](mapName)
        imap.set(key, putIfAbsent, ttl, unit)
      }
      existing
    }
  }
  final class PutIfAbsentEP[K, V](val putIfAbsent: V)
      extends SingleEntryCallbackUpdater[K, V, V] {
    def onEntry(entry: Entry[K, V]): V = {
      val existing = entry.value
      if (existing == null) {
        entry.value = putIfAbsent
      }
      existing
    }
  }
  final class TTLSetIfAbsentEP[K, V](val mapName: String, val putIfAbsent: V, val ttl: Long, val unit: TimeUnit)
      extends SingleEntryCallbackReader[K, V, Boolean]
      with HazelcastInstanceAware {
    @BeanProperty @transient
    var hazelcastInstance: HazelcastInstance = _
    def onEntry(key: K, existing: V): Boolean = {
      val set = existing == null
      if (set) {
        val imap = hazelcastInstance.getMap[K, V](mapName)
        imap.set(key, putIfAbsent, ttl, unit)
      }
      set
    }
  }
  final class SetIfAbsentEP[K, V](val putIfAbsent: V)
      extends SingleEntryCallbackUpdater[K, V, Boolean] {
    def onEntry(entry: Entry[K, V]): Boolean = {
      val set = entry.value == null
      if (set) {
        entry.value = putIfAbsent
      }
      set
    }
  }
}
