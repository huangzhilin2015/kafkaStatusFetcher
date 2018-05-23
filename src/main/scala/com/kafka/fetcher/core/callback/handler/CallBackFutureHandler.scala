package com.kafka.fetcher.core.callback.handler

import org.apache.kafka.common.protocol.Errors

/**
  * Created by huangzhilin on 2018-05-21.
  */
trait CallBackFutureHandler[T] {

  def onSuccess(context: T)

  def onFailure(context: T, errors: Errors)

}
