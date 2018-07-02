package com.kafka.fetcher.core

import java.io.FileNotFoundException
import java.net.{InetSocketAddress, URL}
import java.util
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors
import java.util.{Collections, List, Properties}

import com.kafka.fetcher.exception.{ConfigInvalidException, GroupIdInvalidException, SourceNotFoundException, ValidRequestNotFoundException}
import com.kafka.fetcher.util.Logging
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.apache.kafka.clients.{ClientUtils, _}
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.network._
import org.apache.kafka.common.utils.{LogContext, Time, Utils}
import org.apache.kafka.common.{Cluster, Node}

import scala.util.Random

object KafkaMonitor extends Logging {
  private[core] var zkUtils: ZkUtils = null
  private[core] var config: KafkaMonitorConfig = null
  @volatile private var networkClient: NetworkClient = null
  private var clientPoll: Map[String, List[NetworkClient]] = Map()
  @volatile private var inited: Boolean = false
  //缓存GroupCoordinator之后，不再主动确认节点状态，变更只由回调状态触发
  private var coordinatorCache: scala.collection.mutable.Map[String, Node] = scala.collection.mutable.Map()
  //nodes通过zookeeper获取
  private val nodes: List[Node] = new util.ArrayList[Node]

  private def markedForInitialized(): Unit = inited = true


  private[core] def isInitialized(): Boolean = inited

  /**
    * 初始化kafkamonitor
    */
  private[core] def initMonitorContext(): Unit = {
    val url: URL = this.getClass.getResource("/KafkaMonitorConfig.properties")
    if (url == null) {
      throw new FileNotFoundException("classpath:/KafkaMonitorConfig.properties")
    }
    config = createConfig(url.getFile)
    zkUtils = initZk()
    initNodes()
    initClient(nodes)
    markedForInitialized()
  }

  private def initNodes(): Unit = {
    val listenerName: ListenerName = new ListenerName(config.listenerName)
    zkUtils.getAllBrokersInCluster().foreach(broker => {
      nodes.add(broker.getNode(listenerName))
    })
    if (nodes.size() <= 0) {
      throw new SourceNotFoundException("could not find nodes from zookeeper")
    }
  }

  /**
    * 初始化client
    */
  private def initClient(nodes: util.List[Node]): Unit = {
    val addresses: util.List[InetSocketAddress] = parseAndValidteAddress()
    if (config.clientMode == DefaultConfig.CLIENT_PROVIDE_MODE_SIGLETON) {
      networkClient = createClient(addresses)
    } else if (config.clientMode == DefaultConfig.cLIENT_PROVIDE_MODE_MULTIPLE) {
      //每个broker缓存可配置数量的networkClient,每个client包含各自的channel
      nodes.forEach(node => {
        val clients: util.List[NetworkClient] = new util.ArrayList[NetworkClient]()
        for (index <- 1 to config.clientPollSize) {
          debug(s"begin to init ${index} networkClient,total is ${config.clientPollSize}")
          clients.add(createClient(addresses))
        }
        clientPoll += (node.idString() -> clients)
        nodes.add(node)
      })
    } else {
      throw new ConfigInvalidException(s"config key of ${KafkaMonitorConfig.CLIENT_PROVIDE_MODE} is invalid," +
        s"must be ${DefaultConfig.CLIENT_PROVIDE_MODE_SIGLETON} or ${DefaultConfig.cLIENT_PROVIDE_MODE_MULTIPLE}")
    }
  }

  def restartClient(): NetworkClient = {
    debug("restart client.")
    val addresses: util.List[InetSocketAddress] = parseAndValidteAddress()
    networkClient = createClient(addresses)
    networkClient
  }

  /**
    * 获取一个可用的netWorkClient
    * 根据client 未完成request数量选取
    *
    * @return
    */
  private[core] def findClient(node: Node, time: Time = Time.SYSTEM): NetworkClient = {
    KafkaMonitor.this.synchronized {
      if (config.clientMode == DefaultConfig.cLIENT_PROVIDE_MODE_MULTIPLE) {
        val clients: List[NetworkClient] = clientPoll.get(node.idString()) match {
          case Some(clients) => clients
          case None => null
        }
        val partitonedClients: util.Map[java.lang.Boolean, List[NetworkClient]] = clients.stream()
          .collect(Collectors.partitioningBy((client: NetworkClient) => client.isReady(node, time.milliseconds())))
        val notReadClients = partitonedClients.get(false)
        if (notReadClients != null && notReadClients.size() > 0) {
          //连接channel放到NetworkClientUtils.awaitReady处理
          return notReadClients.get(0)
        }
        val readyClients = partitonedClients.get(true)
        if (readyClients != null && readyClients.size() > 0) {
          return readyClients.stream().min((a, b) => a.inFlightRequestCount(node.idString()) - b.inFlightRequestCount(node.idString())).get()
        }
      } else {
        return networkClient
      }
    }
    throw new ValidRequestNotFoundException(s"could not find a valid request for node ${node.toString}")
  }

  private def parseAndValidteAddress(): util.List[InetSocketAddress] = {
    val brokers: util.List[String] = new util.ArrayList[String]()
    nodes.forEach(node => {
      brokers.add(node.host() + ":" + node.port())
    })
    ClientUtils.parseAndValidateAddresses(brokers)
  }

  /**
    * 初始化networkClient
    *
    * @return
    */
  private def createClient(addresses: util.List[InetSocketAddress], time: Time = Time.SYSTEM): NetworkClient = {
    val metadataUpdater: Metadata = new Metadata(1000L, 300000L, true)
    val logContext = new LogContext("echatKafkaMonitorLog")
    val channelBuilder = ClientUtils.createChannelBuilder(config)
    metadataUpdater.update(Cluster.bootstrap(addresses), Collections.emptySet[String], time.milliseconds)
    val metricTags: java.util.Map[String, String] = Collections.singletonMap("client-id", "cient-id-echat")
    val metricConfig: MetricConfig = new MetricConfig().samples(config.getInt(KafkaMonitorConfig.METRICS_NUM_SAMPLES_CONFIG))
      .timeWindow(config.getLong(KafkaMonitorConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
      .recordLevel(Sensor.RecordingLevel.forName(config.getString(KafkaMonitorConfig.METRICS_RECORDING_LEVEL_CONFIG))).tags(metricTags)
    val reporters: List[MetricsReporter] = new util.ArrayList
    reporters.add(new JmxReporter("echat-kafka-monitor"))
    val metrics: Metrics = new Metrics(metricConfig, reporters, time)
    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      metrics,
      time,
      "echat-kafkamonitor",
      channelBuilder,
      logContext
    )
    new NetworkClient(
      selector,
      metadataUpdater,
      "echat-kafkamonitor-client-id",
      1,
      config.reconnectBackoffMs,
      config.reconnectBackoffMaxMs,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      config.requestTimeOut,
      time,
      false,
      new ApiVersions,
      logContext
    )
  }


  /**
    * 获取GroupCoordinator
    *
    * @param groupId
    * @return
    */
  def coordinator(groupId: String): Node = {
    if (groupId == null)
      throw new GroupIdInvalidException("groupId could not be null")
    val node: Option[Node] = coordinatorCache.get(groupId)
    node match {
      case Some(node) => node
      case None => null
    }
  }

  /**
    * 获取一个node
    *
    * @return
    */
  def leastLoadedNode(): Node = {
    nodes.get(Random.nextInt(nodes.size()))
  }

  def getNodes(): util.List[Node] = {
    nodes
  }

  /**
    * 缓存GroupCoordinator
    *
    * @param groupId
    * @param coordinator
    */
  def cacheCoordinator(groupId: String, coordinator: Node): Unit = {
    KafkaMonitor.this.synchronized {
      coordinatorCache += (groupId -> coordinator)
    }
  }

  /**
    * GroutCoordinator失效
    *
    * @param groupId
    */
  def coordinatorDead(groupId: String): Unit = {
    KafkaMonitor.this.synchronized {
      coordinatorCache.remove(groupId)
    }
  }

  /**
    * 初始化monitorConfig
    *
    * @param fileName
    * @return
    */
  private[this] def createConfig(fileName: String): KafkaMonitorConfig = {
    debug(s"begin create config from file ${fileName}")
    KafkaMonitorConfig.fromProps(getPropsFromArgs(fileName))
  }

  /**
    * 根据配置文件获取properties
    *
    * @param fileName
    * @return
    */
  private[this] def getPropsFromArgs(fileName: String): Properties = {
    //TODO 当前使用一个配置文件
    Utils.loadProps(fileName)
  }

  /**
    * 根据此kafka在zookeeper的存储节点获取zookeeper连接
    *
    * @param time
    * @return
    */
  private[this] def initZk(time: Time = Time.SYSTEM): ZkUtils = {
    debug(s"Connecting to zookeeper on ${config.zkConnect}")
    val chrootIndex = config.zkConnect.indexOf("/")
    val chrootOption = {
      if (chrootIndex > 0) Some(config.zkConnect.substring(chrootIndex))
      else None
    }

    //检查节点
    chrootOption.foreach { chroot =>
      val zkConnForChrootCreation = config.zkConnect.substring(0, chrootIndex)
      val zkClientForChrootCreation = ZkUtils.withMetrics(zkConnForChrootCreation,
        sessionTimeout = config.zkSessionTimeout,
        connectionTimeout = config.zkConnectTimeout,
        false,
        time)
      if (!zkClientForChrootCreation.pathExists(chroot)) {
        //如果节点不存在抛出异常
        throw new ZkNoNodeException("node chroot is not exists")
      }
      zkClientForChrootCreation.close()
    }
    ZkUtils.withMetrics(config.zkConnect,
      sessionTimeout = config.zkSessionTimeout,
      connectionTimeout = config.zkConnectTimeout,
      false,
      time)
  }
}
