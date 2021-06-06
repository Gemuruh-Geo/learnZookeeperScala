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
    }
  }
}
