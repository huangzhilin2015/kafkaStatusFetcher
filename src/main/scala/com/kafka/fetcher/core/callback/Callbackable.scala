package com.kafka.fetcher.core.callback

import com.kafka.fetcher.core.callback.future.CallBackFuture
import com.kafka.fetcher.core.callback.handler.CallBackFutureHandler
import com.kafka.fetcher.util.Logging
import org.apache.kafka.clients.RequestCompletionHandler
import org.apache.kafka.common.protocol.Errors

/**
  * Created by huangzhilin on 2018-05-21.
  */
trait Callbackable[T] extends RequestCompletionHandler with Logging {
  var future: CallBackFuture[T] = new CallBackFuture[T]

  def addFutureListener(handler: CallBackFutureHandler[T]): Callbackable[T] = {
    future.addListener(handler)
    this
  }

  def handleComplete(context: T, errors: Errors): Unit = {
    if (errors != null) {
      future.fireFailure(context, errors)
    } else {
      future.fireSuccess(context)
    }
  }
}
