package sdp.logging

import org.slf4j.Logger

/**
  * Logger which just wraps org.slf4j.Logger internally.
  *
  * @param logger logger
  */
class Log(logger: Logger) {

  // use var consciously to enable squeezing later
  var isDebugEnabled: Boolean = logger.isDebugEnabled
  var isInfoEnabled: Boolean = logger.isInfoEnabled
  var isWarnEnabled: Boolean = logger.isWarnEnabled
  var isErrorEnabled: Boolean = logger.isErrorEnabled

  def withLevel(level: Symbol)(msg: => String, e: Throwable = null): Unit = {
    level match {
      case 'debug | 'DEBUG => debug(msg)
      case 'info | 'INFO => info(msg)
      case 'warn | 'WARN => warn(msg)
      case 'error | 'ERROR => error(msg)
      case _ => // nothing to do
    }
  }

  def debug(msg: => String): Unit = {
    if (isDebugEnabled && logger.isDebugEnabled) {
      logger.debug(msg)
    }
  }

  def debug(msg: => String, e: Throwable): Unit = {
    if (isDebugEnabled && logger.isDebugEnabled) {
      logger.debug(msg, e)
    }
  }

  def info(msg: => String): Unit = {
    if (isInfoEnabled && logger.isInfoEnabled) {
      logger.info(msg)
    }
  }

  def info(msg: => String, e: Throwable): Unit = {
    if (isInfoEnabled && logger.isInfoEnabled) {
      logger.info(msg, e)
    }
  }

  def warn(msg: => String): Unit = {
    if (isWarnEnabled && logger.isWarnEnabled) {
      logger.warn(msg)
    }
  }

  def warn(msg: => String, e: Throwable): Unit = {
    if (isWarnEnabled && logger.isWarnEnabled) {
      logger.warn(msg, e)
    }
  }

  def error(msg: => String): Unit = {
    if (isErrorEnabled && logger.isErrorEnabled) {
      logger.error(msg)
    }
  }

  def error(msg: => String, e: Throwable): Unit = {
    if (isErrorEnabled && logger.isErrorEnabled) {
      logger.error(msg, e)
    }
  }

}