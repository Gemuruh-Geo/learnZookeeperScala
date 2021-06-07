package com.geo

import java.io.IOException

import org.apache.zookeeper.Watcher.Event
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooDefs, ZooKeeper}
import scala.jdk.CollectionConverters._


/**
 * @author Gemuruh Geo Pratama
 * @created 06/06/2021-9:44 AM
 */
case class LeaderElection private (zooKeeperAddress: String, sessionTimeout: Int) extends Watcher{
  val ELECTION_NAMESPACE: String = "/election"
  val TARGET_ZNODE_NAME: String = "/target_znode"
  case class Zk() extends ZooKeeper(zooKeeperAddress, sessionTimeout,this)

  var zk: Option[Zk] = None

  def connectToZookeeper(): Unit = {
    zk = try {
      Some(Zk())
    }catch {
      case e: IOException => None
    }
  }
  def run(): Unit = {
    for {
      zooKeeper <- zk
    }yield zooKeeper.synchronized {
      zooKeeper.wait()
    }
  }

  private def volunteerForLeadership(): Option[String] = {
    println("Volunteer for leader")
    val zNodePrefix = s"$ELECTION_NAMESPACE/c_"
    for {
      zooKeeper <- zk
    }yield zooKeeper.create(zNodePrefix, Array[Byte](),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL)
  }
  def electLeader(): Unit = {
    val volunteerData = volunteerForLeadership()
    for {
      fullPath <- volunteerData
      zooKeeper <- zk
    } yield {
      val smallestChildNode = fullPath.replace(s"$ELECTION_NAMESPACE/","")
      val leaderNode =  zooKeeper.getChildren(ELECTION_NAMESPACE, false).asScala.sortWith(_ < _).head
      if(leaderNode == smallestChildNode) {
        println("I am A leader")
      }else println(s"I am not the leader $leaderNode is a leader")
    }
  }

  def watchTargetZnode(): Unit = {
    zk match {
      case Some(zooKeeper) =>
        val stat = zooKeeper.exists(TARGET_ZNODE_NAME, this)
        if(stat!=null) {
          //if Exists
          //Get Data
          val data: Array[Byte] = zooKeeper.getData(TARGET_ZNODE_NAME,this, stat)
          val children: List[String] = zooKeeper.getChildren(TARGET_ZNODE_NAME, this).asScala.toList

          println(s"Data: ${if(data!=null) new String(data) else ""} children $children")
        }
    }
  }

  override def process(event: WatchedEvent): Unit = {
    import org.apache.zookeeper.Watcher.Event.EventType
    event.getType match {
      case EventType.None => {
        event.getState match {
          case Event.KeeperState.SyncConnected =>
            println("Successfully connected to zookeeper")
          case _ =>
            println("Disconnected Event accept")
            for {
              zooKeeper <- zk
            }yield zooKeeper.synchronized {
              zooKeeper.notifyAll()
            }
        }
      }
      case EventType.NodeCreated =>
        println(s"$TARGET_ZNODE_NAME was created")
      case EventType.NodeDeleted =>
        println(s"$TARGET_ZNODE_NAME was deleted")
      case EventType.NodeDataChanged =>
        println(s"$TARGET_ZNODE_NAME data changed")
      case EventType.NodeChildrenChanged =>
        println(s"$TARGET_ZNODE_NAME children changed")
    }
    watchTargetZnode()
  }
}
