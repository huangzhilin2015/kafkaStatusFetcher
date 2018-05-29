package com.kafka.fetcher.core.callback

import java.util

import com.kafka.fetcher.core.callback.handler.context.CommonContext
import org.apache.kafka.clients.{ClientResponse, NetworkClient}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ListOffsetResponse
import org.apache.kafka.common.requests.ListOffsetResponse.PartitionData
import org.apache.kafka.common.{Node, TopicPartition}

import scala.collection.JavaConverters._

/**
  * Created by huangzhilin on 2018-05-18.
  */
class ListOffsetResponseHandler(groupId: String, node: Node, client: NetworkClient) extends Callbackable[CommonContext] {
  var errors: List[Errors] = List()
  var result: util.Map[TopicPartition, OffsetData] = new util.HashMap()

  override def onComplete(response: ClientResponse) = {
    debug(s"Recieve listOffset resonpse ${response}")
    val listOffsetResponse = response.responseBody.asInstanceOf[ListOffsetResponse]
    for ((k: TopicPartition, v: PartitionData) <- listOffsetResponse.responseData().asScala) {
      val e: Errors = v.error
      if (e eq Errors.NONE) {
        if (v.offsets ne null) {
          // 处理v0版本响应
          //nothing to do
        } else {
          // 处理 v1 及其以后版本的响应
          if (v.offset.longValue() != ListOffsetResponse.UNKNOWN_OFFSET) {
            result.put(k, new OffsetData(v.offset, v.timestamp))
          }
        }
      } else {
        errors.:+(e)
        debug(s"occur error for TopicPartition ${k} . detail: ${e}")
      }
    }
    handleComplete(new CommonContext(groupId, node, client), errors)
  }


}

class OffsetData {
  def this(offsetV: java.lang.Long, timestampV: java.lang.Long) {
    this()
    this.offset = offsetV
    this.timestamp = timestampV
  }

  var offset: java.lang.Long = null
  var timestamp: java.lang.Long = null
}
