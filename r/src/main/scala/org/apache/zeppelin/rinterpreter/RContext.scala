/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.rinterpreter

import java.io._
import java.nio.file.{Files, Paths}
import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.api.r.RBackendHelper
import org.apache.spark.sql.SQLContext
import org.apache.zeppelin.interpreter._
import org.apache.zeppelin.rinterpreter.rscala.RClient._
import org.apache.zeppelin.rinterpreter.rscala._
import org.apache.zeppelin.scheduler._
import org.apache.zeppelin.spark.{SparkInterpreter, ZeppelinContext}
import org.slf4j._

import scala.collection.JavaConversions._

// TODO:  Setup rmr, etc.
// TODO:  Stress-test spark.  What happens on close?  Etc.

/**
 * Created by aelberg on 7/26/15.
 */
private[rinterpreter] class RContext(private val sockets: ScalaSockets,
                                     debug: Boolean) extends RClient(sockets.in, sockets.out, debug) {

  private val logger: Logger = RContext.logger
  lazy val getScheduler: Scheduler = SchedulerFactory.singleton().createOrGetFIFOScheduler(this.hashCode().toString)

  val backend: RBackendHelper = RBackendHelper()
  private var sc: Option[SparkContext] = None
  private var sql: Option[SQLContext] = None
  private var z: Option[ZeppelinContext] = None
  private var interpreterGroup: Option[InterpreterGroup] = null

  val rPkgMatrix = collection.mutable.HashMap[String,Boolean]()

  var isOpen: Boolean = false
  private var isFresh : Boolean = true

  private var property: Properties = null
  private[rinterpreter] var sparkRStarted : Boolean = false

  override def toString() : String = s"""${super.toString()}
       |\t Open: $isOpen Fresh: $isFresh SparkStarted: $sparkRStarted
       |\t Progress: $progress
       |\t Sockets: ${sockets.toString()}
     """.stripMargin

  var progress: Int = 0

  def getProgress: Int = {
    return progress
  }

  def setProgress(i: Int) : Unit = {
    progress = i % 100
  }

  def incrementProgress(i: Int) : Unit = {
    progress = (progress + i) % 100
  }

  // handle properties this way so it can be a mutable object shared with the R Interpreters
  def setProperty(properties: Properties): Unit = synchronized {
    if (property == null) property = properties
    else property.putAll(properties)
  }

  def setInterpreterGroup(group: InterpreterGroup) : InterpreterGroup = {
    interpreterGroup = Some(group)
    group
  }


  def open(startSpark : Boolean): Unit = synchronized {
    isOpen match {
      case true if sparkRStarted =>  logger.info("Reusing rContext.")
      case true if !sparkRStarted & startSpark & interpreterGroup != null => {
        logger.info("Trying to start spark")
        try {
          sparkStartup()
        } catch {
          case e : Exception => {
            logger.error("RContext: Failed to start Spark. This usually means SPARK_HOME isn't set properly", e)
          }
        }
      }
      case true => logger.info("Open called, but no interpreter group")
      case false => {
        testRPackage("rzeppelin", fail = true, message =
          "The rinterpreter cannot run without the rzeppelin package, which was included in your distribution.")
        if (startSpark && interpreterGroup != null) try {
          sparkStartup()
        } catch {
          case e : Exception => {
            logger.error("RContext: Failed to start Spark. This usually means SPARK_HOME isn't set properly", e)
          }
        }
        isOpen = true
        isFresh = false
      }
    }
  }

  private def sparkStartup(): Unit = {
    val sparkHome: String = System.getenv("SPARK_HOME") match {
      case null => {
        logger.error("SPARK_HOME is not set. The R Interpreter will start without Spark.")
        return
      }
      case x => x
    }

    testRPackage("SparkR", fail = true, path = sparkHome)

    def findSparkInterpreter: Option[SparkInterpreter] = {
      def sparkInterpreterRecurse(intp: Interpreter): Option[SparkInterpreter] = intp match {
        case s: SparkInterpreter => Some(s)
        case l: LazyOpenInterpreter => {
          l.open()
          sparkInterpreterRecurse(l.getInnerInterpreter)
        }
        case w: WrappedInterpreter => sparkInterpreterRecurse(w.getInnerInterpreter)
        case _ => None
      }

      interpreterGroup match {
        case Some(x: InterpreterGroup) => x.find(_.getClassName() == "org.apache.zeppelin.spark.SparkInterpreter") match {
          case Some(x: Interpreter) => sparkInterpreterRecurse(x)
          case None => None
        }
        case None => throw new RuntimeException("No interpretergroup, cannot find SparkInterpreter")
      }
    }

    findSparkInterpreter match {
      case Some(intp: SparkInterpreter) => {
        if (!intp.getSparkVersion().isSparkRSupported) throw new RuntimeException("SparkR requires Spark 1.4 or later")
        sc = Some(intp.getSparkContext())
        sql = Some(intp.getSQLContext())
        z = Some(intp.getZeppelinContext())
        logger.info("Registered Spark Contexts")
      }
      case None => throw new RuntimeException("SparkR ould not find a SparkInterpreter and therefore have no SparkContext")
    }

    backend.init()
    backend.start()
    if (!backend.backendThread.isAlive) throw new RuntimeException("SparkR could not startup because the Backend Thread is not alive")
    logger.debug("Started Spark Backend")
    eval( s"""SparkR:::connectBackend("localhost", ${backend.port})""")
    logger.info("SparkR backend connected")


    initializeSparkR(sc.get, sql.get, z.get)
    logger.info("Initialized SparkR")
    sparkRStarted = true
  }

  private def initializeSparkR(sc : SparkContext, sql : SQLContext, z : ZeppelinContext) : Unit = synchronized {

    logger.debug("Getting a handle to the JavaSparkContext")

    eval("assign(\".scStartTime\", as.integer(Sys.time()), envir = SparkR:::.sparkREnv)")
    RStatics.setSC(sc)
    eval(
      """
        |assign(
        |".sparkRjsc",
        |SparkR:::callJStatic("org.apache.zeppelin.rinterpreter.RStatics",
        | "getJSC"),
        | envir = SparkR:::.sparkREnv)""".stripMargin)

    eval("assign(\"sc\", get(\".sparkRjsc\", envir = SparkR:::.sparkREnv), envir=.GlobalEnv)")

    logger.info("Established SparkR Context")

    val sqlEnvName = sql match {
      case null => throw new RuntimeException("Tried to initialize SparkR without setting a SQLContext")
      case x : org.apache.spark.sql.hive.HiveContext => ".sparkRHivesc"
      case x : SQLContext => ".sparkRSQLsc"
    }
    RStatics.setSQL(sql)
    eval(
      s"""
        |assign(
        |"${sqlEnvName}",
        |SparkR:::callJStatic("org.apache.zeppelin.rinterpreter.RStatics",
        | "getSQL"),
        | envir = SparkR:::.sparkREnv)""".stripMargin)
    eval(
      s"""
         |assign("sqlContext",
         |get("$sqlEnvName",
         |envir = SparkR:::.sparkREnv),
         |envir = .GlobalEnv)
       """.stripMargin)

    logger.info("Proving spark")
    val proof = evalS1("names(SparkR:::.sparkREnv)")
    logger.info("Proof of spark is : " + proof.mkString)

    RStatics.setZ(z)
    eval(
    s"""
       |assign(".zeppelinContext",
       |  SparkR:::callJStatic("org.apache.zeppelin.rinterpreter.RStatics",
       | "getZ"),
       | envir = .GlobalEnv)
     """.stripMargin
    )
    logger.debug("Set the ZeppelinContext")

    RStatics.setrCon(this)
    eval(
      s"""
         |assign(".rContext",
         |  SparkR:::callJStatic("org.apache.zeppelin.rinterpreter.RStatics",
         | "getRCon"),
         | envir = .GlobalEnv)
     """.stripMargin
    )
    logger.debug("Set the RContext")
  }

  def close(): Unit = synchronized {
    if (isOpen) {
      if (sparkRStarted) {
        try {
          eval("SparkR:::sparkR.stop()")
        } catch {
          case e: RException => {}
          case e: Exception => logger.error("Error closing SparkR", e)
        }
      }
      try {
        backend.close
        backend.backendThread.stop()
      } catch {
        case e: Exception => logger.error("Error closing RContext ", e)
      }
      try {
        exit()
      } catch {
        case e: Exception => logger.error("Shutdown error", e)
      }
    }
    isOpen = false
  }


  private[rinterpreter] def testRPackage(pack: String,
                                         fail: Boolean = false,
                                         license: Boolean = false,
                                         message: String = "",
                                          path : String = ""): Boolean = synchronized {


    rPkgMatrix.get(pack) match {
      case Some(x: Boolean) => return x
      case None => {}
    }

    evalB0( s"""require('$pack',quietly=TRUE, lib.loc="$path/R/lib/")""") match {
      case true => {
        rPkgMatrix.put(pack, true)
        return (true)
      }
      case false => {
        evalB0(s"require('$pack', quietly=TRUE)") match {
          case true => {
            rPkgMatrix.put(pack, true)
            return true
          }
          case false => {
            rPkgMatrix.put(pack, false)
            val failMessage =
              s"""The $pack package could not be loaded. """ + {
                if (license) "We cannot install it for you because it is published under the GPL3 license."
                else ""
              } + message
            logger.error(failMessage)
            if (fail) throw new RException(failMessage)
            return (false)
          }
        }
      }
    }
  }

  logger.info("RContext Finished Starting")
}

object RContext {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  logger.debug("Inside the RContext Object")

  private[rinterpreter] var lastRContext : Option[RContext] = None

  private[rinterpreter] def resetRcon() : Boolean = {
    val rcon : Option[RContext] = lastRContext
    lastRContext = None
    rcon match {
      case Some(x : RContext) => {
        x.close()
        if (x.isOpen) throw new RuntimeException("Failed to close an existing RContext")
        true
      }
      case None => {false}
    }
  }

  def apply( property: Properties): RContext = synchronized {
        lastRContext match {
          case Some(x : RContext) if x.isFresh => return(x)
          case Some(x : RContext) if x.isOpen => return(x)
          case Some(x : RContext) => resetRcon()
          case _ => {}
        }
        val debug: Boolean = property.getProperty("rscala.debug", "false").toBoolean
        val timeout: Int = property.getProperty("rscala.timeout", "60").toInt
        import scala.sys.process._
        logger.info("Creating processIO")
        var cmd: PrintWriter = null
        val command = RClient.defaultRCmd +: RClient.defaultArguments
        val processCmd = Process(command)

        val processIO = new ProcessIO(
          o => {
            cmd = new PrintWriter(o)
          },
          reader("STDOUT DEBUG: "),
          reader("STDERR DEBUG: "),
          true
        )
        val portsFile = File.createTempFile("rscala-", "")
        val processInstance = processCmd.run(processIO)
        // Find rzeppelin
        val libpath : String = if (Files.exists(Paths.get("R/lib"))) "R/lib"
        else if (Files.exists(Paths.get("../R/lib"))) "../R/lib"
        else throw new RuntimeException("Could not find rzeppelin - it must be in either R/lib or ../R/lib")
        val snippet =
          s"""
library(lib.loc="$libpath", rzeppelin)
rzeppelin:::rServe(rzeppelin:::newSockets('${portsFile.getAbsolutePath.replaceAll(File.separator, "/")}',debug=${if (debug) "TRUE" else "FALSE"},timeout=${timeout}))
q(save='no')"""
        while (cmd == null) Thread.sleep(100)
        cmd.println(snippet)
        cmd.flush()
        val sockets = RClient.makeSockets(portsFile.getAbsolutePath)
        sockets.out.writeInt(RClient.Protocol.OK)
        sockets.out.flush()
        val packVersion = RClient.readString(sockets.in)
        if (packVersion != org.apache.zeppelin.rinterpreter.rscala.Version) {
          logger.warn("Connection to R started but versions don't match " + packVersion + " " + org.apache.zeppelin.rinterpreter.rscala.Version)
        } else {
          logger.info("Connected to a new R Session")
        }
        val context = new RContext(sockets, debug)
        context.setProperty(property)
        lastRContext = Some(context)
        context
  }
}

