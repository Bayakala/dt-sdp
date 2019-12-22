package sdp.logging

import org.slf4j.LoggerFactory

trait LogSupport {

  /**
    * Logger
    */
  protected val log = new Log(LoggerFactory.getLogger(this.getClass))

}