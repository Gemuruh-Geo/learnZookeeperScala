package com.geo

import java.io.IOException

import org.apache.zookeeper.Watcher.Event
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}


/**
 * @author Gemuruh Geo Pratama
 * @created 06/06/2021-9:44 AM
 */
case class LeaderElection private (zooKeeperAddress: String, sessionTimeout: Int) extends Watcher{

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
