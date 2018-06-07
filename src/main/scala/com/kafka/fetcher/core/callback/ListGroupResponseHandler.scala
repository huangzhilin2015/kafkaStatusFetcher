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
  var errors: List[Errors] = List()

  override def onComplete(response: ClientResponse) = {
    debug(s"Received ListGroup response ${response}")
    if (response.hasResponse) {
      val listGroupResonse: ListGroupsResponse = response.responseBody().asInstanceOf[ListGroupsResponse]
      if (listGroupResonse.error() eq Errors.NONE) {
        result = listGroupResonse.groups().asScala.map(group => GroupOverview(group.groupId, group.protocolType)).toList
      } else {
        errors = errors.:+(listGroupResonse.error())
      }
    }
    handleComplete(new CommonContext(null, node, client), errors)
  }
}
