package com.kafka.fetcher.core.callback

import java.util

import com.kafka.fetcher.core.callback.handler.context.CommonContext
import org.apache.kafka.clients.{ClientResponse, NetworkClient}
import org.apache.kafka.common.Node
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.DescribeGroupsResponse
import org.apache.kafka.common.requests.DescribeGroupsResponse.GroupMetadata

/**
  * Created by huangzhilin on 2018-05-28.
  */
class DescribeGroupsResponseHandler(groupId: String, node: Node, client: NetworkClient) extends Callbackable[CommonContext] {
  var result: util.Map[String, GroupMetadata] = new util.HashMap()
  var errors: List[Errors] = List()

  override def onComplete(response: ClientResponse) = {
    val resp: DescribeGroupsResponse = response.responseBody().asInstanceOf[DescribeGroupsResponse]
    resp.groups().forEach((k: String, v: GroupMetadata) => {
      if (v.error() eq Errors.NONE) {
        result.put(k, v)
      } else {
        errors.:+(v.error())
      }
    })
    handleComplete(new CommonContext(groupId, node, client), errors)
  }
}
