package com.kafka.fetcher.core

import java.net.SocketTimeoutException
import java.util

import com.kafka.fetcher.core.callback.{FetchCommitedOffsetResponseHandler, GroupCoordinatorResponseHandler, ListOffsetResponseHandler, OffsetData}
import com.kafka.fetcher.core.request.RequestFactory
import com.kafka.fetcher.exception.CoordinatorNotFoundException
import com.kafka.fetcher.util.Logging
import kafka.cluster.Broker
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.{ClientRequest, NetworkClient, NetworkClientUtils}
import org.apache.kafka.common.requests.{IsolationLevel, ListOffsetRequest}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{Node, TopicPartition}

import scala.collection.JavaConverters._

/**
  * 对外提供的获取kafka监控信息的接口，所有的请求需要指定一个节点
  * 根据groupId获取对应的GroupCoordinator
  * 把每个groupId对应的GroupCoordinator缓存起来，可以作为元数据对外提供
  * Created by huangzhilin on 2018-05-17.
  */
object ExternalEntrance extends Logging {
  def main(arrays: Array[String]): Unit = {
    init()
    var nodes: util.List[Broker] = getSurvivalNodes()
    debug("nodes:" + nodes)
  }

  def init(): Unit = {
    KafkaMonitor.initMonitorContext()
  }

  private def validMonitor(): Unit = {
    if (!KafkaMonitor.isInitialized()) {
      KafkaMonitor.synchronized(
        if (!KafkaMonitor.isInitialized()) {
          KafkaMonitor.initMonitorContext()
        }
      )
    }
  }

  /**
    * 获取服务端当前offsets位置
    *
    * @param groupId
    * @param topicPartitions
    */
  def getCommitedOffset(groupId: String, topicPartitions: util.List[TopicPartition]): util.Map[TopicPartition, OffsetAndMetadata] = {
    validMonitor
    val node: Node = ensureCoordinator(groupId)
    val client: NetworkClient = validContext(node)
    val request: ClientRequest = RequestFactory.getFetchCommitedOffsetRequest(client, node, groupId, topicPartitions)
    NetworkClientUtils.sendAndReceive(client, request, Time.SYSTEM)
    val res: FetchCommitedOffsetResponseHandler = request.callback().asInstanceOf[FetchCommitedOffsetResponseHandler]
    res.result
  }

  /**
    * 获取存活的节点
    */
  def getSurvivalNodes(): util.List[Broker] = {
    validMonitor
    KafkaMonitor.zkUtils.getAllBrokersInCluster().asJava
  }

  /**
    * 获取log最大offset
    *
    * @param groupId
    * @param topicPartitions
    */
  def getHighWaterMarkOffset(groupId: String, topicPartitions: util.List[TopicPartition]): util.Map[TopicPartition, OffsetData] = {
    validMonitor
    val node: Node = ensureCoordinator(groupId)
    val client = validContext(node)
    val request: ClientRequest = RequestFactory.getListListOffsetRequest(client, node, topicPartitions, IsolationLevel.READ_COMMITTED, ListOffsetRequest.LATEST_TIMESTAMP)
    NetworkClientUtils.sendAndReceive(client, request, Time.SYSTEM)
    val res: ListOffsetResponseHandler = request.callback().asInstanceOf[ListOffsetResponseHandler]
    res.result
  }

  /**
    * 获取 log 最大已提交(transactional)位移
    *
    * @param groupId
    * @param topicPartitions
    */
  def getLastStableOffset(groupId: String, topicPartitions: util.List[TopicPartition]): Unit = {
    validMonitor
  }

  /**
    * 获取GroupCoordinator
    *
    * @param groupId
    * @return
    */
  def getCoordinator(groupId: String): Node = {
    validMonitor
    val node: Node = KafkaMonitor.leastLoadedNode()
    val client = validContext(node)
    val request: ClientRequest = RequestFactory.getLookUpCoordinatorRequest(client, node, groupId)
    NetworkClientUtils.sendAndReceive(client, request, Time.SYSTEM)
    val res: GroupCoordinatorResponseHandler = request.callback().asInstanceOf[GroupCoordinatorResponseHandler]
    res.coordinator
  }

  /**
    * 保证请求上下文已经就绪
    *
    * @param time
    */
  private def validContext(node: Node, time: Time = Time.SYSTEM, timeout: Int = config.requestTimeOut): NetworkClient = {
    val client = KafkaMonitor.findClient(node)
    //channel创建成功会更新client/ready状态
    if (!NetworkClientUtils.awaitReady(client, node, time, timeout))
      throw new SocketTimeoutException(s"Failed to connect within $timeout ms")
    client
  }

  /**
    * 获取GroupCoordinator
    * 由于没有心跳，也没有动态更新
    * 所以此节点可能已经过期，使用前需要检测是否已经准备好接受请求
    *
    * @param groupId
    */
  def ensureCoordinator(groupId: String): Node = {
    validMonitor
    /*if (!KafkaMonitor.isInitialized())
      throw new KafkaMonitorNotInitializedException("kafkaMonitor not initialized.")*/
    var node: Node = KafkaMonitor.coordinator(groupId)
    if (node == null) {
      //尝试获取coordinator
      debug(s"coordinator for ${groupId} from cache is null, fetch from server.")
      node = getCoordinator(groupId)
      if (node == null) {
        throw new CoordinatorNotFoundException(s"GroupCoordinator for groupId->${groupId} not found.")
      }
      debug(s"find coordinator[${node}] for ${groupId} from server.")
    } else {
      debug(s"find coordinator[${node}] for ${groupId} from cache.")
    }
    node
  }
}
