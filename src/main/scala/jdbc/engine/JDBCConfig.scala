package sdp.jdbc.config
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.language.implicitConversions
import com.typesafe.config._
import java.util.concurrent.TimeUnit
import java.util.Properties
import scalikejdbc.config._
import com.typesafe.config.Config
import com.zaxxer.hikari._
import scalikejdbc.ConnectionPoolFactoryRepository

/** Extension methods to make Typesafe Config easier to use */
class ConfigExtensionMethods(val c: Config) extends AnyVal {
  import scala.collection.JavaConverters._

  def getBooleanOr(path: String, default: => Boolean = false) = if(c.hasPath(path)) c.getBoolean(path) else default
  def getIntOr(path: String, default: => Int = 0) = if(c.hasPath(path)) c.getInt(path) else default
  def getStringOr(path: String, default: => String = null) = if(c.hasPath(path)) c.getString(path) else default
  def getConfigOr(path: String, default: => Config = ConfigFactory.empty()) = if(c.hasPath(path)) c.getConfig(path) else default

  def getMillisecondsOr(path: String, default: => Long = 0L) = if(c.hasPath(path)) c.getDuration(path, TimeUnit.MILLISECONDS) else default
  def getDurationOr(path: String, default: => Duration = Duration.Zero) =
    if(c.hasPath(path)) Duration(c.getDuration(path, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS) else default

  def getPropertiesOr(path: String, default: => Properties = null): Properties =
    if(c.hasPath(path)) new ConfigExtensionMethods(c.getConfig(path)).toProperties else default

  def toProperties: Properties = {
    def toProps(m: mutable.Map[String, ConfigValue]): Properties = {
      val props = new Properties(null)
      m.foreach { case (k, cv) =>
        val v =
          if(cv.valueType() == ConfigValueType.OBJECT) toProps(cv.asInstanceOf[ConfigObject].asScala)
          else if(cv.unwrapped eq null) null
          else cv.unwrapped.toString
        if(v ne null) props.put(k, v)
      }
      props
    }
    toProps(c.root.asScala)
  }

  def getBooleanOpt(path: String): Option[Boolean] = if(c.hasPath(path)) Some(c.getBoolean(path)) else None
  def getIntOpt(path: String): Option[Int] = if(c.hasPath(path)) Some(c.getInt(path)) else None
  def getStringOpt(path: String) = Option(getStringOr(path))
  def getPropertiesOpt(path: String) = Option(getPropertiesOr(path))
}

object ConfigExtensionMethods {
  @inline implicit def configExtensionMethods(c: Config): ConfigExtensionMethods = new ConfigExtensionMethods(c)
}

trait HikariConfigReader extends TypesafeConfigReader {
  self: TypesafeConfig =>      // with TypesafeConfigReader => //NoEnvPrefix =>

  import ConfigExtensionMethods.configExtensionMethods

  def getFactoryName(dbName: Symbol): String = {
    val c: Config = config.getConfig(envPrefix + "db." + dbName.name)
    c.getStringOr("poolFactoryName", ConnectionPoolFactoryRepository.COMMONS_DBCP)
  }

  def hikariCPConfig(dbName: Symbol): HikariConfig = {

    val hconf = new HikariConfig()
    val c: Config = config.getConfig(envPrefix + "db." + dbName.name)

    // Connection settings
    if (c.hasPath("dataSourceClass")) {
      hconf.setDataSourceClassName(c.getString("dataSourceClass"))
    } else {
      Option(c.getStringOr("driverClassName", c.getStringOr("driver"))).map(hconf.setDriverClassName _)
    }
    hconf.setJdbcUrl(c.getStringOr("url", null))
    c.getStringOpt("user").foreach(hconf.setUsername)
    c.getStringOpt("password").foreach(hconf.setPassword)
    c.getPropertiesOpt("properties").foreach(hconf.setDataSourceProperties)

    // Pool configuration
    hconf.setConnectionTimeout(c.getMillisecondsOr("connectionTimeout", 1000))
    hconf.setValidationTimeout(c.getMillisecondsOr("validationTimeout", 1000))
    hconf.setIdleTimeout(c.getMillisecondsOr("idleTimeout", 600000))
    hconf.setMaxLifetime(c.getMillisecondsOr("maxLifetime", 1800000))
    hconf.setLeakDetectionThreshold(c.getMillisecondsOr("leakDetectionThreshold", 0))
    hconf.setInitializationFailFast(c.getBooleanOr("initializationFailFast", false))
    c.getStringOpt("connectionTestQuery").foreach(hconf.setConnectionTestQuery)
    c.getStringOpt("connectionInitSql").foreach(hconf.setConnectionInitSql)
    val numThreads = c.getIntOr("numThreads", 20)
    hconf.setMaximumPoolSize(c.getIntOr("maxConnections", numThreads * 5))
    hconf.setMinimumIdle(c.getIntOr("minConnections", numThreads))
    hconf.setPoolName(c.getStringOr("poolName", dbName.name))
    hconf.setRegisterMbeans(c.getBooleanOr("registerMbeans", false))

    // Equivalent of ConnectionPreparer
    hconf.setReadOnly(c.getBooleanOr("readOnly", false))
    c.getStringOpt("isolation").map("TRANSACTION_" + _).foreach(hconf.setTransactionIsolation)
    hconf.setCatalog(c.getStringOr("catalog", null))

    hconf

  }
}

import scalikejdbc._
trait ConfigDBs {
  self: TypesafeConfigReader with TypesafeConfig with HikariConfigReader =>

  def setup(dbName: Symbol = ConnectionPool.DEFAULT_NAME): Unit = {
    getFactoryName(dbName) match {
      case "hikaricp" => {
        val hconf = hikariCPConfig(dbName)
        val hikariCPSource = new HikariDataSource(hconf)
        case class HikariDataSourceCloser(src: HikariDataSource) extends DataSourceCloser {
          var closed = false
          override def close(): Unit = src.close()
        }
        if (hconf.getDriverClassName != null && hconf.getDriverClassName.trim.nonEmpty) {
          Class.forName(hconf.getDriverClassName)
        }
        ConnectionPool.add(dbName, new DataSourceConnectionPool(dataSource = hikariCPSource,settings = DataSourceConnectionPoolSettings(),
          closer = HikariDataSourceCloser(hikariCPSource)))
      }
      case _ => {
        val JDBCSettings(url, user, password, driver) = readJDBCSettings(dbName)
        val cpSettings = readConnectionPoolSettings(dbName)
        if (driver != null && driver.trim.nonEmpty) {
          Class.forName(driver)
        }
        ConnectionPool.add(dbName, url, user, password, cpSettings)
      }
    }
  }

  def setupAll(): Unit = {
    loadGlobalSettings()
    dbNames.foreach { dbName => setup(Symbol(dbName)) }
  }

  def close(dbName: Symbol = ConnectionPool.DEFAULT_NAME): Unit = {
    ConnectionPool.close(dbName)
  }

  def closeAll(): Unit = {
    ConnectionPool.closeAll
  }

}


object ConfigDBs extends ConfigDBs
  with TypesafeConfigReader
  with StandardTypesafeConfig
  with HikariConfigReader

case class ConfigDBsWithEnv(envValue: String) extends ConfigDBs
  with TypesafeConfigReader
  with StandardTypesafeConfig
  with HikariConfigReader
  with EnvPrefix {

  override val env = Option(envValue)
}