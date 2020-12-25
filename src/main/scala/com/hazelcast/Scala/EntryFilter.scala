package com.hazelcast.Scala

import scala.language.higherKinds

import com.hazelcast.query.Predicate
import collection.mutable.Map

sealed abstract class EntryFilter[K, V] {
  type EV
  type M[R]
}
sealed abstract class MultiEntryFilter[K, V]
extends EntryFilter[K, V] {
  type EV = V
  type M[R] = Map[K, R]
}

final case class OnEntries[K, V](predicate: Predicate[K, V] = null)
extends MultiEntryFilter[K, V]

final case class OnValues[K, V](include: V => Boolean)
extends MultiEntryFilter[K, V]

final case class OnKeys[K, V](keys: collection.Set[K])
extends MultiEntryFilter[K, V]
object OnKeys {
  def apply[K, V](key1: K, key2: K, keyn: K*) = new OnKeys[K, V](keyn.toSet + key1 + key2)
}

final case class OnKey[K, V](key: K)
extends EntryFilter[K, V] {
  type EV = Option[V]
  type M[R] = R
}
