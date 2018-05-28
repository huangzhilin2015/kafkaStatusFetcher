package com.kafka.fetcher.core.base

import com.kafka.fetcher.core.KafkaMonitor

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
}
