package com.digdeep.util.concurrent

import java.util.concurrent.atomic.AtomicBoolean

/**
 * Created by denismo on 18/09/15.
 */
class ThreadOnce {
  private val called = new ThreadLocal[AtomicBoolean]() {
    override def initialValue(): AtomicBoolean = new AtomicBoolean()
  }
  def callOnce(callable:  => Unit): Unit = {
    if (called.get().compareAndSet(false, true)) {
      callable
    }
  }
  def reset() = called.get().set(false)
}
