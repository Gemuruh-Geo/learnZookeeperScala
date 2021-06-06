package com.geo

import org.apache.zookeeper.ZooKeeper

/**
 * @author Gemuruh Geo Pratama
 * @created 04/06/2021-10:43 AM
 */
object Main extends App {
  val ZOOKEEPER_ADDRESS: String = "localhost:2181"
  val SESSION_TIMEOUT: Int = 30000
  val leaderElection = LeaderElection(zooKeeperAddress = ZOOKEEPER_ADDRESS, sessionTimeout = SESSION_TIMEOUT)
  leaderElection.connectToZookeeper()
  leaderElection.run()
}
