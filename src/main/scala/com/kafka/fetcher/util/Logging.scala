package com.kafka.fetcher.util

import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
  * Created by huangzhilin on 2018-05-15.
  */
trait Logging {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

  protected def debug(message: => String) = {
    if (logger.isDebugEnabled) {
      logger.debug(message)
    }
  }

  protected def info(message: => String) = {
    if (logger.isInfoEnabled()) {
      logger.info(message)
    }
  }

  protected def error(message: => String) = {
    if (logger.isErrorEnabled()) {
      logger.error(message)
    }
  }

  protected def error(message: => String, exception: => Throwable) = {
    if (logger.isErrorEnabled()) {
      logger.error(message, exception)
    }
  }
}
