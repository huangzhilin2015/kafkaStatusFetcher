package com.kafka.fetcher.core.callback.future

import java.util.concurrent.ConcurrentLinkedQueue

import com.kafka.fetcher.core.callback.handler.CallBackFutureHandler
import org.apache.kafka.common.protocol.Errors

import scala.util.control._

/**
  * Created by huangzhilin on 2018-05-22.
  */
class CallBackFuture[T] {
  var listeners: ConcurrentLinkedQueue[CallBackFutureHandler[T]] = new ConcurrentLinkedQueue[CallBackFutureHandler[T]]()
  val breaker: Breaks = new Breaks

  def addListener(listener: CallBackFutureHandler[T]): Unit = {
    listeners.add(listener)
  }

  def fireSuccess(context: T): Unit = {
    breaker.breakable(
      while (true) {
        val handler: CallBackFutureHandler[T] = listeners.poll();
        if (handler == null) {
          breaker.break()
        }
        handler.onSuccess(context)
      }
    )
  }

  def fireFailure(context: T, errors: Errors): Unit = {
    breaker.breakable(
      while (true) {
        val handler: CallBackFutureHandler[T] = listeners.poll();
        if (handler == null) {
          breaker.break()
        }
        handler.onFailure(context, errors)
      }
    )
  }
}
