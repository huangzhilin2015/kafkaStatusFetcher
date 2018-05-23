package com.kafka.fetcher.core

import java.net.InetSocketAddress
import java.util
import java.util.{Collections, List, Properties}
import java.util.concurrent.TimeUnit

import kafka.network.RequestChannel
import kafka.server.{KafkaApis, KafkaConfig, ReplicaFetcherBlockingSend}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.{Cluster, Node}
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.network.{ChannelBuilder, ListenerName}
import org.apache.kafka.common.security.JaasContext.Type
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.utils.{Bytes, LogContext, Time}

/*import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStream, KTable, Materialized, Produced}
import org.apache.kafka.streams.state.KeyValueStore*/

import kafka.tools.ConsoleConsumer
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.apache.kafka.clients._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.{ChannelBuilders, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.requests.AbstractRequest.Builder
import org.apache.kafka.common.requests.{FindCoordinatorRequest, ListOffsetRequest}
import org.apache.kafka.common.security.{JaasContext, JaasUtils}

import scala.collection.Map

/**
  * 只作为单独的请求接口调用，不维持心跳和coordinator
  * 每次请求前都需要明确coordinator节点，否则认作请求失败
  * Created by huangzhilin on 2018-05-07.
  */
object Test {
  val zkConnect: String = "192.168.139.129:2181"
  val zkSessionTimeoutMs: Int = 3000
  val zkConnectionTimeoutMs: Int = 3000
  val groupId: String = "echat-kafkamonitor"
  val baseGroupId: String = "echat-task"
  val bootstarp: String = "192.168.139.129:9092"

  def main(args: Array[String]): Unit = {
    /*println("testf")
    start();
    val logContext = new LogContext(s"[Controller id ")
    val serverProps: Properties = new Properties()
    val config: KafkaConfig = KafkaConfig.fromProps(serverProps)*/
    val zkUtils: ZkUtils = initZk()
    println(zkUtils.pathExists("/delete_topics"))
    println(zkUtils.pathExists("/brokers-11"))
    val networkClient: NetworkClient = initKafkaClient(zkUtils = zkUtils)
    val response: ClientResponse = NetworkClientUtils.sendAndReceive(networkClient, getLookUpCoordinatorRequest(networkClient), Time.SYSTEM)
    println("666")
  }

  /**
    * 初始化zookeeper,并且检查节点
    * 暂时不启用acl验证
    *
    * @param time
    * @return
    */
  def initZk(time: Time = Time.SYSTEM): ZkUtils = {
    println(s"Connecting to zookeeper on ${zkConnect}")

    val chrootIndex = zkConnect.indexOf("/")
    val chrootOption = {
      if (chrootIndex > 0) Some(zkConnect.substring(chrootIndex))
      else None
    }

    /* val secureAclsEnabled = config.zkEnableSecureAcls
     val isZkSecurityEnabled = JaasUtils.isZkSecurityEnabled()

     if (secureAclsEnabled && !isZkSecurityEnabled)
       throw new java.lang.SecurityException(s"${KafkaConfig.ZkEnableSecureAclsProp} is true, but the verification of the JAAS login file failed.")
 */
    //检查节点
    chrootOption.foreach { chroot =>
      val zkConnForChrootCreation = zkConnect.substring(0, chrootIndex)
      val zkClientForChrootCreation = ZkUtils.withMetrics(zkConnForChrootCreation,
        sessionTimeout = zkSessionTimeoutMs,
        connectionTimeout = zkConnectionTimeoutMs,
        /*isZkSecurityEnabled*/ false,
        time)
      if (!zkClientForChrootCreation.pathExists(chroot)) {
        //如果节点不存在抛出异常
        throw new ZkNoNodeException("node chroot is not exists")
      }
      zkClientForChrootCreation.close()
    }
    ZkUtils.withMetrics(zkConnect,
      sessionTimeout = zkSessionTimeoutMs,
      connectionTimeout = zkConnectionTimeoutMs,
      /*secureAclsEnabled*/ false,
      time)
  }

  /**
    * 初始化netWorkClient
    *
    * @param time
    * @param zkUtils
    * @return
    */
  def initKafkaClient(/*config: KafkaConfig, */ time: Time = Time.SYSTEM, zkUtils: ZkUtils): NetworkClient = {
    val curBrokers = zkUtils.getAllBrokersInCluster().toSet
    val metadataUpdater: Metadata = new Metadata(100L, 300000L, true)
    metadataUpdater.update(Cluster.bootstrap(ClientUtils.parseAndValidateAddresses(util.Arrays.asList(bootstarp))), Collections.emptySet[String], time.milliseconds)
    //val metadataUpdater = new ManualMetadataUpdater()
    val logContext = new LogContext(s"[Controller id ")
    /*val channelBuilder = ChannelBuilders.clientChannelBuilder(
      config.interBrokerSecurityProtocol,
      JaasContext.Type.SERVER,
      config,
      config.interBrokerListenerName,
      config.saslMechanismInterBrokerProtocol,
      config.saslInterBrokerHandshakeRequestEnable
    )*/
    val metricTags: java.util.Map[String, String] = Collections.singletonMap("client-id", "cient-id-loren")
    val metricConfig: MetricConfig = new MetricConfig().samples(2 /*config.getInt(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG)*/)
      .timeWindow(/*config.getLong(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG)*/ 30000L, TimeUnit.MILLISECONDS)
      .recordLevel(Sensor.RecordingLevel.forName(/*config.getString(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG)*/ "INFO")).tags(metricTags)
    val reporters: List[MetricsReporter] = new util.ArrayList
    reporters.add(new JmxReporter("echat-kafka-monitor"))
    val metrics: Metrics = new Metrics(metricConfig, reporters, time)
    //val channelBuilder = ClientUtils.createChannelBuilder(config)
    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      metrics,
      time,
      "echat-kafkamonitor",
      null,
      logContext
    )
    new NetworkClient(
      selector,
      metadataUpdater,
      /* config.brokerId.toString*/ "echat-kafkamonitor-client-id",
      1,
      0,
      0,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      /*config.requestTimeoutMs*/ 30000,
      time,
      false,
      new ApiVersions,
      logContext
    )
  }

  /**
    * 获取请求coordinator节点的request
    */
  def getLookUpCoordinatorRequest(client: NetworkClient): ClientRequest = {
    val requestBuilder = new FindCoordinatorRequest.Builder(FindCoordinatorRequest.CoordinatorType.GROUP, this.baseGroupId)
    val node: Node = client.leastLoadedNode(Time.SYSTEM.milliseconds())
    client.ready(node, System.currentTimeMillis())
    client.newClientRequest(node.idString(), requestBuilder, System.currentTimeMillis(), true)
  }

  def findCoordinator(client: NetworkClient): Node = {
    val requestBuilder = new FindCoordinatorRequest.Builder(FindCoordinatorRequest.CoordinatorType.GROUP, this.baseGroupId)
    val node: Node = client.leastLoadedNode(Time.SYSTEM.milliseconds())
    client.ready(node, System.currentTimeMillis())
    client.newClientRequest(node.idString(), requestBuilder, System.currentTimeMillis(), true)
    null
  }

  def getRequest(client: NetworkClient): ClientRequest = {
    val topicPartition: TopicPartition = new TopicPartition("test-0", 0)
    val earliestOrLatest: java.lang.Long = ListOffsetRequest.LATEST_TIMESTAMP
    val partitions = Map(topicPartition -> (earliestOrLatest: java.lang.Long))
    //client.newClientRequest(ListOffsetRequest.Builder.forReplica(1, 1).setTargetTimes(partitions.asInstanceOf[java.util.Map[TopicPartition, java.lang.Long]]))
    null
  }

  def fetchOffsetLogSIze: () => Long = () => {
    val topicPartition: TopicPartition = new TopicPartition("test-0", 0)
    val earliestOrLatest: java.lang.Long = ListOffsetRequest.LATEST_TIMESTAMP
    val partitions = Map(topicPartition -> (earliestOrLatest: java.lang.Long))
    val requestBuilder = ListOffsetRequest.Builder.forReplica(1, 1)
      .setTargetTimes(partitions.asInstanceOf[java.util.Map[TopicPartition, java.lang.Long]])
    2L
  }

  def fetchOffset(): Unit = {
  }

  /*  def start(): Unit = {
      val config: Properties = {
        val prop = new Properties()
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "loren_test_" + System.currentTimeMillis())
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.107:9092")
        prop.put("auto.offset.reset", "earliest")
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
        prop
      }
      val builder: StreamsBuilder = new StreamsBuilder
      //__consumer_offsets
      builder.stream("__consumer_offsets", Consumed.`with`(Serdes.String(), Serdes.String()))
        .peek((key, value) => {
          println("key=" + key + ",value=" + value)
        })
      //topic
      val ks: KafkaStreams = new KafkaStreams(builder.build(), config)
      println("ks.start.begin.")
      ks.start()
      println("ks.start.end.")
      Runtime.getRuntime.addShutdownHook(new Thread(() => {
        println("jvm shutdown!")
        ks.close(10, TimeUnit.SECONDS)
      }))
      println("START.")
    }*/
}
