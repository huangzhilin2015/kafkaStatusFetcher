package com.kafka.fetcher.exception

/**
  * Created by huangzhilin on 2018-05-21.
  */
class ValidRequestNotFoundException(message: String) extends RuntimeException(message: String) {
  def this() {
    this(null)
  }

}
