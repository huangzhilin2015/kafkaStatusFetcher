package com.kafka.fetcher.core.callback.handler

import com.kafka.fetcher.core.callback.handler.context.CommonContext
import com.kafka.fetcher.util.Logging
import org.apache.kafka.common.protocol.Errors

/**
  * Created by huangzhilin on 2018-05-21.
  */
class ClientStatusUpdateHandler extends CallBackFutureHandler[CommonContext] with Logging {

  override def onSuccess(context: CommonContext): Unit = {
    debug("ClientStatusUpdateHandler .onSuccess.handle")
  }

  override def onFailure(context: CommonContext, errors: List[Errors]): Unit = {
    debug("ClientStatusUpdateHandler .onfailure.handle")
    var close: Boolean = false
    errors.foreach(error => {
      if ((error eq Errors.BROKER_NOT_AVAILABLE) || (error eq Errors.NETWORK_EXCEPTION)) {
        close = true
      }
    })
    if (close) {
      context.client.close(context.node.idString())
    }
  }

}
