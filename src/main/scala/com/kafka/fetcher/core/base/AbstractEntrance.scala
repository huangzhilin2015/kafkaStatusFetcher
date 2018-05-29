package com.kafka.fetcher.core.base

import java.net.SocketTimeoutException

import com.kafka.fetcher.core.KafkaMonitor
import org.apache.kafka.clients.{NetworkClient, NetworkClientUtils}
import org.apache.kafka.common.Node
import org.apache.kafka.common.utils.Time

/**
  * Created by huangzhilin on 2018-05-28.
  */
trait AbstractEntrance {
  protected def init(): Unit = {
    KafkaMonitor.initMonitorContext()
  }

  protected def validMonitor(): Unit = {
    if (!KafkaMonitor.isInitialized()) {
      KafkaMonitor.synchronized(
        if (!KafkaMonitor.isInitialized()) {
          KafkaMonitor.initMonitorContext()
        }
      )
    }
  }

  protected def validContext(node: Node, time: Time = Time.SYSTEM, timeout: Int = KafkaMonitor.config.requestTimeOut): NetworkClient = {
    val client = KafkaMonitor.findClient(node)
    //channel创建成功会更新client/ready状态
    if (!NetworkClientUtils.awaitReady(client, node, time, timeout))
      throw new SocketTimeoutException(s"Failed to connect within $timeout ms")
    client
  }
}
