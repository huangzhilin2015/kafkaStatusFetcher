package com.kafka.fetcher.core.callback

import org.apache.kafka.clients.{ClientResponse, RequestCompletionHandler}

/**
  * 测试请求回调
  * Created by huangzhilin on 2018-05-17.
  */
class RequestFutureCompletionHandler extends RequestCompletionHandler {
  override def onComplete(response: ClientResponse) = {
    println("回调成功!response=" + response.toString)
  }
}
