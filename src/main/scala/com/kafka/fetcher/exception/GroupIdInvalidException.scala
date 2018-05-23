package com.kafka.fetcher.exception

/**
  * Created by huangzhilin on 2018-05-17.
  */
class GroupIdInvalidException(message: String) extends RuntimeException(message: String) {
  def this() {
    this(null)
  }

}
