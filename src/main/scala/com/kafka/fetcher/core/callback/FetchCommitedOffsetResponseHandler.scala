package com.kafka.fetcher.core.callback

import java.util

import com.kafka.fetcher.core.KafkaMonitor
import com.kafka.fetcher.core.callback.handler.context.CommonContext
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.{ClientResponse, NetworkClient}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.OffsetFetchResponse
import org.apache.kafka.common.{Node, TopicPartition}

import scala.collection.JavaConverters._

/**
  * Created by huangzhilin on 2018-05-18.
  */
class FetchCommitedOffsetResponseHandler(groupId: String, node: Node, client: NetworkClient) extends Callbackable[CommonContext] {
  var errors: List[Errors] = List()
  var result: util.Map[TopicPartition, OffsetAndMetadata] = null

  override def onComplete(response: ClientResponse): Unit = {
    debug(s"Received OffsetFetched response ${response}")
    val offsetFetchResponse = response.responseBody.asInstanceOf[OffsetFetchResponse]
    if (offsetFetchResponse.hasError) {
      errors = errors.:+(offsetFetchResponse.error());
      debug(s"Expected error in fetch offset response:${offsetFetchResponse.error().message} ")
    } else {
      val offsets: util.Map[TopicPartition, OffsetAndMetadata] = new util.HashMap[TopicPartition, OffsetAndMetadata](offsetFetchResponse.responseData.size)
      for ((k: TopicPartition, v: OffsetFetchResponse.PartitionData) <- offsetFetchResponse.responseData.asScala) {
        if (v.hasError) {
          val error = v.error
          debug(s"Failed to fetch offset for partition${k}:${error.message}")
          if (error eq Errors.UNKNOWN_TOPIC_OR_PARTITION) {
            debug(s"Topic of Partition ${k} does not exist.")
          } else {
            debug(s"Unexpected error in fetch offset response: ${error.message()}")
          }
          errors = errors.:+(error)
        } else if (v.offset >= 0) {
          offsets.put(k, new OffsetAndMetadata(v.offset, v.metadata))
        } else {
          debug(s"Found no committed offset for partition ${k}")
        }
      }
      result = offsets
    }
    handleComplete(new CommonContext(groupId, node, client), errors)
  }
}
