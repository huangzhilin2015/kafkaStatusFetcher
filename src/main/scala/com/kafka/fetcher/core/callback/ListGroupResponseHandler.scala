package com.kafka.fetcher.core.callback

import com.kafka.fetcher.core.callback.handler.context.CommonContext
import kafka.coordinator.group.GroupOverview
import org.apache.kafka.clients.{ClientResponse, NetworkClient}
import org.apache.kafka.common.Node
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ListGroupsResponse

import scala.collection.JavaConverters._

/**
  * Created by huangzhilin on 2018-05-25.
  */
class ListGroupResponseHandler(node: Node, client: NetworkClient) extends Callbackable[CommonContext] {
  var result: List[GroupOverview] = null
  var errors: Errors = null

  override def onComplete(response: ClientResponse) = {
    println("ListGroupResponseHandler.onComplete.")
    println(response)
    if (response.hasResponse) {
      val listGroupResonse: ListGroupsResponse = response.responseBody().asInstanceOf[ListGroupsResponse]
      if ((errors = listGroupResonse.error()) != Errors.NONE) {
        result = listGroupResonse.groups().asScala.map(group => GroupOverview(group.groupId, group.protocolType)).toList
      }
    }
    handleComplete(new CommonContext(null, node, client), errors)
  }
}
