package com.kafka.fetcher.core.callback

import java.util

import com.kafka.fetcher.core.callback.handler.context.CommonContext
import org.apache.kafka.clients.{ClientResponse, NetworkClient}
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.utils.Time


/**
  * Created by huangzhilin on 2018-06-22.
  */
class GetAllTopicsResponseHandler(groupId: String, client: NetworkClient, node: Node, time: Time = Time.SYSTEM) extends Callbackable[CommonContext] {
  var topicMetadata: util.Collection[MetadataResponse.TopicMetadata] = null
  var tpNode: Map[TopicPartition, Node] = Map()

  override def onComplete(response: ClientResponse): Unit = {
    debug("GetAllTopicsResponseHandler.onComplete.")
    val metadataResponse: MetadataResponse = response.responseBody().asInstanceOf[MetadataResponse]
    topicMetadata = metadataResponse.topicMetadata()
    topicMetadata.forEach(metadataResponse => {
      if (metadataResponse.error() eq Errors.NONE) {
        val topic: String = metadataResponse.topic()
        if (!"__consumer_offsets".equals(topic)) {
          metadataResponse.partitionMetadata().forEach(pMetadata => {
            tpNode += (new TopicPartition(topic, pMetadata.partition()) -> pMetadata.leader())
          })
        }
      }
    })
    handleComplete(new CommonContext(groupId, node, client), errors)
  }
}
