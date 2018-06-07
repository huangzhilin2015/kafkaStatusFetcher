package com.kafka.fetcher.core.callback

import com.kafka.fetcher.core.callback.handler.context.CommonContext
import org.apache.kafka.clients.{ClientResponse, NetworkClient}
import org.apache.kafka.common.Node
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.FindCoordinatorResponse
import org.apache.kafka.common.utils.Time

/**
  * 获取GroupCoordinator的回调
  * Created by huangzhilin on 2018-05-17.
  */
class GroupCoordinatorResponseHandler(groupId: String, client: NetworkClient, node: Node, time: Time = Time.SYSTEM) extends Callbackable[CommonContext] {
  var coordinator: Node = null
  var errors: List[Errors] = List()

  override def onComplete(response: ClientResponse): Unit = {
    debug(s"Received GroupCoordinator response ${response}")
    val findCoordinatorResponse = response.responseBody.asInstanceOf[FindCoordinatorResponse]
    val responseErrors = findCoordinatorResponse.error()
    if (responseErrors eq Errors.NONE) {
      coordinator = new Node(Integer.MAX_VALUE - findCoordinatorResponse.node.id,
        findCoordinatorResponse.node.host,
        findCoordinatorResponse.node.port);
      //try connect
      client.ready(coordinator, time.milliseconds())
    } else {
      debug(s"Group coordinator lookup failed: ${responseErrors.message}")
      errors = errors.:+(responseErrors)
      coordinator = null
    }
    handleComplete(new CommonContext(groupId, coordinator, client), errors)
  }
}
