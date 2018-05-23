package com.kafka.fetcher.exception

/**
  * Created by huangzhilin on 2018-05-23.
  */
class SourceNotFoundException(message: String) extends RuntimeException {
  def this() = {
    this(null)
  }

}
