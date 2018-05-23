package com.kafka.fetcher.core.callback.handler

import com.kafka.fetcher.core.KafkaMonitor
import com.kafka.fetcher.core.callback.handler.context.CommonContext
import com.kafka.fetcher.util.Logging
import org.apache.kafka.common.protocol.Errors

/**
  * Created by huangzhilin on 2018-05-22.
  */
class GroupCoordinatorUpdateHandler extends CallBackFutureHandler[CommonContext] with Logging {
  override def onSuccess(context: CommonContext): Unit = {
    debug("GroupCoordinatorUpdateHandler .onsuccess.handle")
    KafkaMonitor.cacheCoordinator(context.groupId, context.node)
  }

  override def onFailure(context: CommonContext, errors: Errors): Unit = {
    debug("GroupCoordinatorUpdateHandler .onfailure.handle")
    if (errors.code() == Errors.COORDINATOR_NOT_AVAILABLE.code() || errors.code() == Errors.NOT_COORDINATOR) {
      KafkaMonitor.coordinatorDead(context.groupId)
    }
  }
}
