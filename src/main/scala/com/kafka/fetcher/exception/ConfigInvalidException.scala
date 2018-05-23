package com.kafka.fetcher.exception

/**
  * Created by huangzhilin on 2018-05-22.
  */
class ConfigInvalidException(message: String) extends RuntimeException(message: String) {
  def this() = {
    this(null)
  }
}
