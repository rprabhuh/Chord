import akka.actor._
import akka.actor.Props
import scala.concurrent.duration.Duration
import com.typesafe.config.ConfigFactory
import scala.math._
import scala.util.control.Breaks._

sealed trait chord

case class Initialize(actorRefs: Array[ActorRef]) extends chord
case class SendRequest(requesterRef: ActorRef, message: String, hopNum: Int) extends chord
//case class FailureKill(timeToSleep: Int) extends chord

class fingertable(startc: Int, intStart: Int, intEnd: Int, nodec: ActorRef) {
  var start: Int = startc
  var interval = (intStart, intEnd)
  var node: ActorRef = nodec

  override def toString(): String = {
  	return "\nStart = " + start + "\nInterval = " + interval + "\nNode = " + node + "\n"
  }
}


// Create an actor for a peer
class Node(idc: Int, m: Int) extends Actor {
  var nodeId = idc
  var successor: ActorRef = null
  var predecessor: ActorRef = null
  var finger:Array[fingertable] = new Array[fingertable](m + 1)
  var i = 0

  def receive = {
    case Initialize(actors) =>
      for(i <- 1 to m) {
        val start = ((nodeId + pow(2, (i-1))) % pow(2,m)).toInt
      	val intEnd = ((nodeId + pow(2, i)) % pow(2,m)).toInt
        var node: ActorRef = null

        // Find finger[i].node = First node -geq finger[i].start
        var sIdx = start
        //println("Start = " + start + "\t" + actors(start))
        while(actors(sIdx) == null) {
        	//println("\nChecking sIdx = " + sIdx + " for " + actors(start))
        	sIdx = (sIdx + 1) % pow(2, m).toInt
        }
        node = actors(sIdx)

        finger(i) =  new fingertable(start, start, intEnd, node);
      }

      successor = finger(1).node
      for(i <- 1 to m)
 	     println("Actor = " + self + " ID: " + nodeId + "\tfinger["+i+"] "+ finger(i).toString())
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
    var numNodes = args(0).toInt
  	val m = ceil(log(numNodes)/log(2)).toInt
  	val size = pow(2, m).toInt

    // Number of requests each peer will make
    var numRequests = args(1)

    val system = ActorSystem("Chord")
    var PeerNodes:Array[ActorRef] = new Array[ActorRef](size)
    var idx = 0

    // Initialize the actor nodes to NULL
    for( i <- 0 until size) {
        PeerNodes(i) = null
    }

    for( i <- 0 until size by 2) {
    	if (idx < numNodes) {
    		PeerNodes(i) = system.actorOf(Props(new Node(i, m)))
    		idx += 1
    	}
    }

    // Actor positions in the network ring - for debugging
    println("Peer Network of size " + size)
    for( i <- 0 until size) {
    	if (PeerNodes(i) != null)
    		println(PeerNodes(i) + " present at " + i)
    }
    println("\n")

    for( i <- 0 until size) {
    	if (PeerNodes(i) != null)
        	PeerNodes(i) ! Initialize(PeerNodes)
    }

  }
}
