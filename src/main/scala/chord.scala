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
case class Create(actors: Array[ActorRef], nodeLocations:Array[Int], numRequests: Int) extends chord
case class JoinNetwork(n: ActorRef) extends chord
case class SendRequest(requesterRef: ActorRef, message: String, hopNum: Int) extends chord
case class GetNodeId() extends chord
case class GetSuccessor() extends chord
case class GetPredecessor() extends chord
case class SetPredecessor(pred: ActorRef) extends chord
case class FindSuccessor(id: Int) extends chord
case class ClosestPrecedingFinger(id: Int) extends chord
case class UpdateFingerTable(s: ActorRef, i: Int) extends chord
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
class Node(idc: Int, mc: Int) extends Actor {
  var nodeId = idc
  val m = mc
  var successor: ActorRef = null
  var predecessor: ActorRef = null
  var finger:Array[fingertable] = new Array[fingertable](m + 1)
  var i = 0
  implicit val timeout = Timeout(30 seconds)
  var future:Future[Any]= null


  // ask node to find its predecessor
  def find_predecessor(id: Int): ActorRef = {
    println("succ " + successor + " of " + self)
  	var succNodeId = -1
    if (successor != self) {
      future = successor ? GetNodeId
      succNodeId = Await.result(future, timeout.duration).asInstanceOf[Int]
    }
    else
      succNodeId = nodeId

    println("sNodeId = " + succNodeId)
  	var queryInterval = (nodeId-1) to succNodeId
  	
  	var nextNode: ActorRef = null
    var n1: ActorRef = self
  	while(queryInterval.contains(id) == false) {

      if (n1 != self) {
        future = n1 ? ClosestPrecedingFinger(id)
        n1 = Await.result(future, timeout.duration).asInstanceOf[ActorRef]
      }
      else
        n1 = ClosestPrecFinger(id)

      var n1Succ: ActorRef = null
      if (n1 != self) {
        future = n1 ? GetSuccessor
        n1Succ = Await.result(future, timeout.duration).asInstanceOf[ActorRef]
      }
      else
        n1Succ = successor

      var n1SuccNodeId: Int = -1
      if (n1Succ != self) {
        future = n1Succ ? GetNodeId
        n1SuccNodeId = Await.result(future, timeout.duration).asInstanceOf[Int]
      }
      else
        n1SuccNodeId = nodeId

      var n1NodeId: Int = -1
      if (n1 != self) {
        future = n1 ? GetNodeId
        n1NodeId = Await.result(future, timeout.duration).asInstanceOf[Int]
      }
      else
        n1NodeId = nodeId

  		queryInterval = (n1NodeId - 1) to n1SuccNodeId
  	}

  	return n1;
  }

  // Hash function
  def hash(key: String): Int = {
    return MH3.stringHash(key)
  }

  def init_finger_table(n1: ActorRef) = {
		if (n1 != self) {
			future = n1 ? FindSuccessor(finger(1).start)
			finger(1).node = Await.result(future, timeout.duration).asInstanceOf[ActorRef]
		}
		else
			finger(1).node = successor

		if (finger(1).node != self) {
			future = finger(1).node ? GetPredecessor()
			predecessor = Await.result(future, timeout.duration).asInstanceOf[ActorRef]
		}

		if (successor != self) {
			future = successor ? SetPredecessor(self)
			val success = Await.result(future, timeout.duration).asInstanceOf[ActorRef]
		}
		else
			predecessor = self

    var fingerNodeId: Int = -1
    for( i <- 1 until m) {

			if (finger(i).node != self) {
				future = finger(i).node ? GetNodeId
				fingerNodeId = Await.result(future, timeout.duration).asInstanceOf[Int]
			}
			else
				fingerNodeId = nodeId

			var queryInterval = nodeId to (fingerNodeId - 1)
  		if (queryInterval.contains(finger(i+1).start))
  			finger(i+1).node = finger(i).node
  		else {
				if (n1 != self) {
					future = n1 ? FindSuccessor(finger(i+1).start)
					finger(i+1).node = Await.result(future, timeout.duration).asInstanceOf[ActorRef]
				}
				else
					finger(i+1).node = successor
  		}
  	}
  }

  def ClosestPrecFinger(id: Int): ActorRef = {
      val queryInterval = ((nodeId - 1) % pow(2, m)).toInt to ((id - 1) % pow(2, m)).toInt
      var node = -1
      for (i <- m to 1 by -1) {
        //println("ClosestPrecFinger: Checking " + nodeId + " in " + queryInterval)
        if (queryInterval.contains(nodeId))
          return finger(i).node
      }
      return self
  }

  def update_others() = {
  	var p: ActorRef = null
  	for( i <- 0 to m) {
  		p = find_predecessor((nodeId - pow(2, i-1)).toInt)
			if (p != self)
  			p ! UpdateFingerTable(self, i)
			else
				UpdFingerTable(self, i)
  	}
  }

	def UpdFingerTable(s: ActorRef, i: Int): Unit = {
		var fingerNodeId = -1
		if (finger(i).node != self) {
			future = finger(i).node ? GetNodeId
			fingerNodeId = Await.result(future, timeout.duration).asInstanceOf[Int]
		}
		else
			fingerNodeId = nodeId

		var sNodeId: Int = -1
		if (s != self) {
			future = s ? GetNodeId
			sNodeId = Await.result(future, timeout.duration).asInstanceOf[Int]
		}
		else
			sNodeId = nodeId

		var queryInterval = nodeId to (fingerNodeId - 1)
		if (queryInterval.contains(sNodeId)) {
			finger(i).node = s
			var p = predecessor
			if (p != self)
				p ! UpdateFingerTable(s, i)
			else
				UpdFingerTable(s, i)
		}
	}


  def receive = {
    case Initialize(actors) =>
      successor = finger(1).node
      for(i <- 1 to m)
 	     println("Actor = " + self + " ID: " + nodeId + "\tfinger["+i+"] "+ finger(i).toString()) 
         
    case JoinNetwork(n1) =>
			println("Node " + nodeId + " joining the network")

      //Create the finger table
      for(i <- 1 to m) {
        val start = ((nodeId + pow(2, (i-1))) % pow(2,m)).toInt
        val intEnd = ((nodeId + pow(2, i)) % pow(2,m)).toInt
        finger(i) =  new fingertable(start, start, intEnd, self);
      }

      for(i<-1 to m)
        print(finger(i).toString())

      if(n1 == null) {
        predecessor = self
        successor = self
      } else {
        init_finger_table(n1)
        update_others()
      }

      sender ! true

 	case GetNodeId() =>
    println("Let me get node id for " + sender)
 		sender ! nodeId

 	case GetSuccessor() =>
 		sender ! successor

 	case GetPredecessor() =>
 		sender ! predecessor

 	case SetPredecessor(pred: ActorRef) =>
 		  predecessor = pred
      sender ! true

 	case FindSuccessor(id: Int) =>
 		val n1 = find_predecessor(id)
    var succResult:ActorRef = null
    if (n1 != self) {
      future = n1 ? GetSuccessor
      succResult = Await.result(future, timeout.duration).asInstanceOf[ActorRef]
    }
    else
      succResult = finger(1).node

		sender ! succResult

 	case ClosestPrecedingFinger(id) =>
 		val queryInterval = (nodeId - 1) to (id - 1)
  		var node = -1
  		for (i <- m to 1 by -1) {		
			future = finger(i).node ? GetNodeId
			val result = Await.result(future, timeout.duration).asInstanceOf[Int]
	
  			if (queryInterval.contains(result)) {
  				sender ! finger(i).node
  			}
  		}	
  		sender ! self

  	case UpdateFingerTable(s: ActorRef, i: Int) =>
  		future = finger(i).node ? GetNodeId
  		var fingerNodeId = Await.result(future, timeout.duration).asInstanceOf[Int]
  		
  		future = s ? GetNodeId
	  	var sNodeId = Await.result(future, timeout.duration).asInstanceOf[Int]
  		
  		var queryInterval = nodeId to (fingerNodeId - 1)
  		if (queryInterval.contains(sNodeId)) {
  			finger(i).node = s
  			var p = predecessor
  			p ? UpdateFingerTable(s, i)
  		}

    case GiveMeSomePlace(key) =>
      sender ! hash(key)

    case _ =>
      println("ERROR: Unknown Message - " + sender)
  }
}


// Create the result listener
class TheArchitect extends Actor {
  def receive = {
    case Create(actors, nodeLocations, numRequests) =>
      implicit val timeout = Timeout(30 seconds)
      var future = actors(0) ? JoinNetwork(null)
      var success = Await.result(future, timeout.duration).asInstanceOf[Boolean]

      actors(nodeLocations(i)) ! JoinNetwork(actors(0))
      /*for(i <- 1 until nodeLocations.length) {
        future = actors(nodeLocations(i)) ? JoinNetwork(actors(0))
        success = Await.result(future, timeout.duration).asInstanceOf[Boolean]
      }*/

    case _ => println("INVALID MESSAGE")
      System.exit(1)
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
    val numNodes = args(0).toInt
  	val m = 10 //ceil(log(numNodes)/log(2)).toInt
  	val size = pow(2, m).toInt

    // Number of requests each peer will make
    var numRequests = args(1).toInt

    val system = ActorSystem("Chord")
    var PeerNodes:Array[ActorRef] = new Array[ActorRef](size)
		var NodeLocations:Array[Int] = new Array[Int](numNodes)
    var idx = 0

    // Initialize the actor nodes to NULL
    for( i <- 0 until size) {
        PeerNodes(i) = null
    }

		val slots = floor(size/numNodes).toInt
		println("Peer Network of size " + size)

		for( i <- 0 until size by slots) {
			if (idx < numNodes) {
				PeerNodes(i) = system.actorOf(Props(new Node(i, m)))
				NodeLocations(idx) = i
				println(PeerNodes(i) + " present at " + i)
				idx += 1
			}
		}

    val Arch = system.actorOf(Props[TheArchitect])
    Arch ! Create(PeerNodes, NodeLocations, numRequests)

    println("\n")

	}
}
