package com.kafka.fetcher.core.callback.handler.context

import org.apache.kafka.clients.NetworkClient
import org.apache.kafka.common.Node

/**
  * Created by huangzhilin on 2018-05-22.
  */
class CommonContext {
  def this(groupId: String, node: Node, client: NetworkClient) = {
    this()
    this.groupId = groupId;
    this.client = client
    this.node = node
  }

  var groupId: String = null
  var client: NetworkClient = null
  var node: Node = null
}
