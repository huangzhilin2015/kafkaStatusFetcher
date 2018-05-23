package com.kafka.fetcher.core

import java.util.Properties

import kafka.utils.Implicits.PropertiesOps
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

/**
  * 默认配置
  */
object DefaultConfig {
  val CLIENT_PROVIDE_MODE_SIGLETON: Int = 1
  val cLIENT_PROVIDE_MODE_MULTIPLE: Int = 2
  //默认值
  val DEFAULT_CLIENT_PROVIDE_MODE: Int = CLIENT_PROVIDE_MODE_SIGLETON

}

object KafkaMonitorConfig {
  val COMMON_REQUEST_TIMEOUT_CONFIG: String = "common.request.timeout"
  val BOOTSTRAP_SERVERS_CONFIG: String = "bootstrap.servers"
  val ZOOKEEPER_CONNECT_CONFIG: String = "zookeeper.connect"
  val CLIENT_POLL_SIZE: String = "client.poll.size"
  val ZOOKEEPER_SESSION_TIMEOUT_CONFIG: String = "zookeeper.session.timeout"
  val ZOOKEEPER_CONNECT_TIMEOUTE_CONFIG: String = "zookeeper.connect.timeout"
  val LISTENER_NAME: String = "listener.name"
  val SECURITY_PROTOCOL_CONFIG: String = "security.protocol"
  val SASL_MECHAISM_CONFIG: String = "sasl.mechanism"
  val RECONNECT_BACKOFF_MS_CONFIG: String = "reconnect.backoff.ms"
  val RECONNECT_BACKOFF_MAX_MS_CONFIG: String = "reconnect.backoff.max.ms"
  val CLIENT_PROVIDE_MODE: String = "client.provide.mode"
  val METRICS_NUM_SAMPLES_CONFIG = "metrics.num.samples"
  val METRICS_SAMPLE_WINDOW_MS_CONFIG = "metrics.sample.window.ms"
  val METRICS_RECORDING_LEVEL_CONFIG = "metrics.recording.level"
  private val configDef: ConfigDef = new ConfigDef().define(
    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
    Type.LIST,
    Importance.HIGH,
    /*CommonClientConfigs.BOOTSTRAP_SERVERS_DOC*/ "")
    .define(KafkaMonitorConfig.ZOOKEEPER_CONNECT_CONFIG,
      Type.STRING,
      Importance.HIGH, "")
    .define(KafkaMonitorConfig.ZOOKEEPER_CONNECT_TIMEOUTE_CONFIG,
      Type.INT,
      null,
      Importance.HIGH,
      "")
    .define(KafkaMonitorConfig.ZOOKEEPER_SESSION_TIMEOUT_CONFIG,
      Type.INT,
      Importance.HIGH,
      "")
    .define(KafkaMonitorConfig.SECURITY_PROTOCOL_CONFIG,
      Type.STRING,
      CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
      Importance.HIGH,
      CommonClientConfigs.SECURITY_PROTOCOL_DOC)
    .define(KafkaMonitorConfig.SASL_MECHAISM_CONFIG,
      Type.STRING,
      "GSSAPI",
      Importance.HIGH,
      "")
    .define(KafkaMonitorConfig.COMMON_REQUEST_TIMEOUT_CONFIG,
      Type.INT,
      10000,
      Importance.HIGH,
      "通用的请求超时时间(毫秒)")
    .define(KafkaMonitorConfig.RECONNECT_BACKOFF_MS_CONFIG,
      Type.INT,
      50,
      Importance.HIGH,
      "")
    .define(KafkaMonitorConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG,
      Type.INT,
      50,
      Importance.HIGH,
      "")
    .define(KafkaMonitorConfig.CLIENT_POLL_SIZE,
      Type.INT,
      5,
      Importance.HIGH,
      "")
    .define(KafkaMonitorConfig.LISTENER_NAME,
      Type.STRING,
      CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
      Importance.HIGH,
      "默认和security.protocol相同,map=PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL:SSL")
    .define(KafkaMonitorConfig.CLIENT_PROVIDE_MODE,
      Type.INT,
      DefaultConfig.DEFAULT_CLIENT_PROVIDE_MODE,
      Importance.HIGH,
      "NetworkClient提供，分为单例模式和多例模式")
    .define(KafkaMonitorConfig.METRICS_NUM_SAMPLES_CONFIG,
      Type.INT,
      1,
      Importance.HIGH,
      "")
    .define(KafkaMonitorConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG,
      Type.LONG,
      30000,
      Importance.HIGH,
      "")
    .define(KafkaMonitorConfig.METRICS_RECORDING_LEVEL_CONFIG,
      Type.STRING,
      "info",
      Importance.HIGH,
      "")


  /**
    * 通过一个properties初始化config，默认doLog为true
    *
    * @param props
    * @return
    */
  def fromProps(props: Properties): KafkaMonitorConfig = {
    fromProps(props, true)
  }

  def fromProps(props: Properties, doLog: Boolean): KafkaMonitorConfig = {
    new KafkaMonitorConfig(props, doLog)
  }

  def fromProps(defaults: Properties, overrides: Properties, doLog: Boolean): KafkaMonitorConfig = {
    val props = new Properties()
    props ++= defaults
    props ++= overrides
    fromProps(props, doLog)
  }

  def apply(props: java.util.Map[_, _]): KafkaMonitorConfig = new KafkaMonitorConfig(props, true)
}

class KafkaMonitorConfig(val props: java.util.Map[_, _], doLog: Boolean) extends AbstractConfig(KafkaMonitorConfig.configDef, props, doLog) {
  def this(props: java.util.Map[_, _]) = this(props, true)

  /** ********* Init Configuration ***********/
  val zkConnect: String = getString(KafkaMonitorConfig.ZOOKEEPER_CONNECT_CONFIG)
  val zkSessionTimeout: Int = getInt(KafkaMonitorConfig.ZOOKEEPER_SESSION_TIMEOUT_CONFIG)
  val zkConnectTimeout: Int = getInt(KafkaMonitorConfig.ZOOKEEPER_CONNECT_TIMEOUTE_CONFIG)
  val bootstrap: java.util.List[String] = getList(KafkaMonitorConfig.BOOTSTRAP_SERVERS_CONFIG)
  val requestTimeOut: Int = getInt(KafkaMonitorConfig.COMMON_REQUEST_TIMEOUT_CONFIG)
  val reconnectBackoffMs: Int = getInt(KafkaMonitorConfig.RECONNECT_BACKOFF_MS_CONFIG)
  val reconnectBackoffMaxMs: Int = getInt(KafkaMonitorConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG)
  val clientPollSize: Int = getInt(KafkaMonitorConfig.CLIENT_POLL_SIZE)
  val listenerName: String = getString(KafkaMonitorConfig.SECURITY_PROTOCOL_CONFIG)
  val clientMode: Int = getInt(KafkaMonitorConfig.CLIENT_PROVIDE_MODE)
}
