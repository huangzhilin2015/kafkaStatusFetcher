package com.kafka.fetcher.core.callback.handler

import com.kafka.fetcher.util.Logging
import org.apache.kafka.common.protocol.Errors

/**
  * Created by huangzhilin on 2018-05-22.
  */
class TestHandler extends CallBackFutureHandler[Int] with Logging {
  override def onSuccess(context: Int): Unit = {
    error(s"TestHandler.onSuccess.context=${context}")
  }

  override def onFailure(context: Int, errors: List[Errors]): Unit = {
    error(s"TestHandler.onFailure.context=${context}")
  }
}
