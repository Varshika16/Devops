package com.tetra.airline.appdata

import com.typesafe.config.ConfigFactory
import scala.util.Properties

import scala.io;
import java.nio.file.{ Paths, Files }
import java.io.File

import scala.beans.{ BeanProperty, BooleanBeanProperty }

class AppConfig {

  
  @BeanProperty
  var ddlSrcPath: String = _

 @BeanProperty
  var ddlStgPath: String = _

  @BeanProperty
  var ddlTgtPath: String = _

  @BeanProperty
  var srcloadPath: String = _

  @BeanProperty
  var tgtloadPath: String = _

 @BeanProperty
  var repSqlPath: String = _

  @BeanProperty
  var pythonFilePath: String = _

    @BeanProperty
  var databaseName: String = _

    @BeanProperty
  var loadIntoSrc: Boolean = _

    @BeanProperty
  var loadIntoStg: Boolean = _

   @BeanProperty
  var runApplicationName: String = _

 @BeanProperty
  var prtionClmn: String = _

    
  def getConfigInfo(fileName: String): AppConfig = {

    val configFile = new File(fileName);

    val fileConfig = ConfigFactory.parseFile(configFile)
    val config = ConfigFactory.load(fileConfig)
    val appConf = new AppConfig()

    appConf.setDdlSrcPath(config.getString("airline.secret.ddlSrcPath"))    
    appConf.setDdlStgPath(config.getString("airline.secret.ddlStgPath"))
    appConf.setDdlTgtPath(config.getString("airline.secret.ddlTgtPath"))
    appConf.setSrcloadPath(config.getString("airline.secret.srcloadPath"))
    appConf.setTgtloadPath(config.getString("airline.secret.tgtloadPath"))
    appConf.setRepSqlPath(config.getString("airline.secret.repSqlPath"))
    appConf.setPythonFilePath(config.getString("airline.secret.pythonFilePath"))
    appConf.setLoadIntoSrc(config.getBoolean("airline.secret.loadIntoSrc"))
    appConf.setLoadIntoStg(config.getBoolean("airline.secret.loadIntoStg"))
    appConf.setDatabaseName(config.getString("airline.secret.databaseName"))  
    appConf.setRunApplicationName(config.getString("airline.secret.runApplicationName"))  
    appConf.setPrtionClmn(config.getString("airline.secret.prtionClmn"))
    appConf
  }

}
