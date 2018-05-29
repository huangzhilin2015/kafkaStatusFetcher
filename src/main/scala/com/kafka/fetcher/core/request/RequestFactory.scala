package com.kafka.fetcher.core.request

import java.util

import com.kafka.fetcher.core.callback.handler.{ClientStatusUpdateHandler, GroupCoordinatorUpdateHandler}
import com.kafka.fetcher.core.callback._
import org.apache.kafka.clients.{ClientRequest, NetworkClient}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{Node, TopicPartition}

import scala.collection.JavaConverters._

/**
  * 获取ClientRequest
  * Created by huangzhilin on 2018-05-17.
  */
object RequestFactory {
  /**
    * 获取ListGroup的request
    *
    * @param client
    * @param node
    * @param time
    * @return
    */
  def getListGroupRequest(client: NetworkClient, node: Node, time: Time = Time.SYSTEM): ClientRequest = {
    val requestBuilder = new ListGroupsRequest.Builder();
    client.newClientRequest(node.idString, requestBuilder, time.milliseconds(), true, new ListGroupResponseHandler(node, client));
  }

  /**
    * 获取group详细信息request
    *
    * @param client
    * @param node
    * @param groupIds
    * @param time
    */
  def getDescripeGroupRequest(client: NetworkClient, node: Node, groupIds: util.List[String], time: Time = Time.SYSTEM): ClientRequest = {
    val requestBuilder = new DescribeGroupsRequest.Builder(groupIds)
    client.newClientRequest(node.idString(), requestBuilder, time.milliseconds(), true, new DescribeGroupsResponseHandler(null, node, client))
  }

  /**
    * 获取coordinator的requst
    *
    * @param client
    * @param node
    * @param groupId
    * @param time
    * @return
    */
  def getLookUpCoordinatorRequest(client: NetworkClient, node: Node, groupId: String, time: Time = Time.SYSTEM): ClientRequest = {
    val requestBuilder = new FindCoordinatorRequest.Builder(FindCoordinatorRequest.CoordinatorType.GROUP, groupId)
    client.newClientRequest(node.idString(),
      requestBuilder,
      time.milliseconds(),
      true,
      new GroupCoordinatorResponseHandler(groupId, client, node, time)
        .addFutureListener(new GroupCoordinatorUpdateHandler())
        .addFutureListener(new ClientStatusUpdateHandler()))
  }

  /**
    * 获取当前消费位置
    *
    * @param client
    * @param node
    * @param groupId
    * @param topicPartitions
    * @param time
    * @return
    */
  def getFetchCommitedOffsetRequest(client: NetworkClient, node: Node, groupId: String, topicPartitions: util.List[TopicPartition], time: Time = Time.SYSTEM): ClientRequest = {
    val requestBuilder = new OffsetFetchRequest.Builder(groupId, topicPartitions)
    client.newClientRequest(node.idString, requestBuilder, time.milliseconds(), true,
      new FetchCommitedOffsetResponseHandler(groupId, node, client)
        .addFutureListener(new ClientStatusUpdateHandler())
        .addFutureListener(new GroupCoordinatorUpdateHandler()))
  }

  /**
    * 获取topicPartition offset 基于consumer端
    * 传入的所有topicPartition使用同一个timestamp
    *
    * @param client
    * @param node
    * @param topicPartitions
    * @param isolationLevel 隔离级别，区别在于事务消息和非事务消息的不同体现
    *                       COMMITED=>事务消息：LSO<=HW，非事务消息：LSO==HW
    *                       UNCOMMITED=>LSO=HW
    * @param timestamp      时间戳 基于大于等于此时间戳的第一条消息
    * @param time
    * @return
    */
  def getListListOffsetRequest(client: NetworkClient, node: Node, topicPartitions: util.List[TopicPartition], isolationLevel: IsolationLevel, timestamp: java.lang.Long, time: Time = Time.SYSTEM): ClientRequest = {
    var target: Map[TopicPartition, java.lang.Long] = Map()

    def buildTarget(tp: TopicPartition): Unit = {
      target += (tp -> timestamp)
    }

    topicPartitions.forEach(tp => buildTarget(tp))
    val requestBuilder = ListOffsetRequest.Builder.forConsumer(false, isolationLevel).setTargetTimes(target.asJava)
    client.newClientRequest(node.idString, requestBuilder, time.milliseconds(), true,
      new ListOffsetResponseHandler(null, node, client).addFutureListener(new ClientStatusUpdateHandler()))
  }


}
