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

  override def onFailure(context: CommonContext, errors: List[Errors]): Unit = {
    debug("GroupCoordinatorUpdateHandler .onfailure.handle")
    var coordinatorDead: Boolean = false
    errors.foreach(error => {
      if ((error eq Errors.COORDINATOR_NOT_AVAILABLE) || (error eq Errors.NOT_COORDINATOR) || (error eq Errors.NOT_LEADER_FOR_PARTITION)) {
        coordinatorDead = true
      }
    })
    if (coordinatorDead) {
      KafkaMonitor.coordinatorDead(context.groupId)
    }
  }
}
