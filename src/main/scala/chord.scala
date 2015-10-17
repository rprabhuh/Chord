import akka.actor._
import akka.actor.Props
import akka.routing.RoundRobinRouter
import scala.concurrent.duration.Duration
import com.typesafe.config.ConfigFactory
import java.security.MessageDigest
import java.io.File
import scala.math._

sealed trait chord

case class Initialize(actorRefs: Array[ActorRef]) extends chord
case class FailureLicenseToKill(nodesRef: Array[ActorRef]) extends chord
case class FailureKill(timeToSleep: Int) extends chord

class fingertable(startc: Int, intStart: Int, intEnd: Int, nodec:Int, successorc:Int, predecessorc:Int) {
  var start: Int = startc
  var interval = (intStart, intEnd)
  var node: Int = nodec
  var successor: Int = successorc
  var predecessor: Int = predecessorc
}


// Create an actor for a peer
class Node(idc: Int, m: Int) extends Actor {
  var id = idc
  var i = 0 
  var finger:Array[fingertable] = new Array[fingertable](m)
  def receive = {
    case Initialize(actors) =>
      var intEnd = (id + 1) % pow(2,m)
      for(i <- 1 to m) {
        val start = intEnd
        val intStart = -1
        intEnd = (id + pow(2, i)) % pow(2,m)
        

        finger(i) =  new fingertable(-1,-1,-1,-1,-1,-1);
      }
      println("Error")
    case _ =>
      println("Error")
  }
}



// Create the result listener
class Listener extends Actor {
  def receive = {
    case _ => println("INVALID MESSAGE")
    System.exit(1)
  }
}


// Create the App
object Chord extends App {
  override def main(args: Array[String]) {

    //Validate Input
    if(args.length != 2) {
      println("ERROR: Invalid number of arguments. Please enter run <numNodes> <numRequests>") 
      System.exit(1)
    }

    // Number of nodes (peers)
    var numNodes = args(0)

    // Number of requests each peer will make
    var numRequests = args(1)

  }
}
