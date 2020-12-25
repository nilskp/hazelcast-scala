package com.hazelcast.Scala.actor

import scala.concurrent.Future
import com.hazelcast.core.HazelcastInstance

trait HzActorRef[A <: AnyRef] {
  def name: String
  def apply[R](thunk: (HazelcastInstance, A) => R): Future[R]
}
