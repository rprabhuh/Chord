import akka.actor._
import akka.actor.Props
import scala.concurrent.duration.Duration
import com.typesafe.config.ConfigFactory
import scala.math._
import scala.util.control.Breaks._
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.hashing.{MurmurHash3=>MH3}


sealed trait chord

case class Initialize(actorRefs: Array[ActorRef]) extends chord
case class JoinNetwork(n: ActorRef) extends chord
case class SendRequest(requesterRef: ActorRef, message: String, hopNum: Int) extends chord
case class GetNodeId() extends chord
case class GetSuccessor() extends chord
case class ClosestPrecedingFinger(id: Int) extends chord
case class GiveMeSomePlace(ip: String) extends chord
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
  implicit val timeout = Timeout(5 seconds)
  var future:Future[Any]= null

  // ask node to find its successor
  def find_successor(id: Int): ActorRef = {
  	var n1 = find_predecessor(id)
  	future = n1 ? GetSuccessor
	val succResult = Await.result(future, timeout.duration).asInstanceOf[ActorRef]

  	return succResult
  }

  // ask node to find its predecessor
  def find_predecessor(id: Int): ActorRef = {
  	var n1 = self
  	var future = n1 ? GetNodeId
	var n1NodeId = Await.result(future, timeout.duration).asInstanceOf[Int]
	future = n1 ? GetSuccessor
	var n1Succ = Await.result(future, timeout.duration).asInstanceOf[ActorRef]
	future = n1Succ ? GetNodeId
	var n1SuccNodeId = Await.result(future, timeout.duration).asInstanceOf[Int]

  	var queryInterval = n1NodeId to n1SuccNodeId
  	
  	var nextNode: ActorRef = null
  	while(queryInterval.contains(id) == false) {
  		//n1 = n1.closest_preceding_finger(id)
  		future = n1 ? ClosestPrecedingFinger
		n1 = Await.result(future, timeout.duration).asInstanceOf[ActorRef]
		
		future = n1 ? GetSuccessor
		n1Succ = Await.result(future, timeout.duration).asInstanceOf[ActorRef]
		future = n1Succ ? GetNodeId
		n1SuccNodeId = Await.result(future, timeout.duration).asInstanceOf[Int]

		future = n1 ? GetNodeId
		n1NodeId = Await.result(future, timeout.duration).asInstanceOf[Int]

  		queryInterval = n1NodeId to n1SuccNodeId
  	}

  	return n1;
  }

  // Hash function
  def hash(key: String): Int = {
    return MH3.stringHash(key)
  }

  def init_finger_table(n: ActorRef) = {
  }
  
  def update_others() = {

  }

  def receive = {
    case Initialize(actors) =>
      successor = finger(1).node
      for(i <- 1 to m)
 	     println("Actor = " + self + " ID: " + nodeId + "\tfinger["+i+"] "+ finger(i).toString()) 
         
    case JoinNetwork(n1) =>
      if(n1 == null) {
        nodeId = hash(self.path.name)
        for(i <- 1 to m) {
          finger(i).node = self 
        }
        predecessor = self
        successor = self
      } else {
        future = n1 ? GiveMeSomePlace(self.path.name)
        nodeId = Await.result(future, timeout.duration).asInstanceOf[Int]
        init_finger_table(n1)
        update_others()
      }

 	case GetNodeId() =>
 		sender ! nodeId

 	case GetSuccessor() =>
 		sender ! successor

 	case ClosestPrecedingFinger(id) =>
 		val queryInterval = nodeId to id
  		var node = -1
  		for (i <- m to 1 by -1) {		
			future = finger(i).node ? GetNodeId
			val result = Await.result(future, timeout.duration).asInstanceOf[Int]
	
  			if (queryInterval.contains(result)) {
  				sender ! finger(i).node
  			}
  		}
	
  		sender ! self

    case GiveMeSomePlace(key) =>
      sender ! hash(key)

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
