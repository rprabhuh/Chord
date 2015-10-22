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
case class JoinNetwork(n: NodeInfo) extends chord
case class SendRequest(requesterRef: ActorRef, message: String, hopNum: Int) extends chord
case class GetNodeId() extends chord
case class GetSuccessor() extends chord
case class GetPredecessor() extends chord
case class SetPredecessor(pred: NodeInfo) extends chord
case class FindSuccessor(id: Int) extends chord
case class ClosestPrecedingFinger(id: Int) extends chord
case class UpdateFingerTable(s: NodeInfo, i: Int) extends chord
case class GiveMeSomePlace(ip: String) extends chord
//case class FailureKill(timeToSleep: Int) extends chord

class NodeInfo(n: ActorRef, id: Int) {
  var node = n
  var nodeId = id

  override def toString(): String = {
    return "\nNode = " + node + "\nID = " + nodeId + "\n"
  }
}


class fingertable(startc: Int, intStart: Int, intEnd: Int, nodec: NodeInfo) {
  var start: Int = startc
  var interval = (intStart, intEnd)
  var node: NodeInfo = nodec

  override def toString(): String = {
  	return "\nStart = " + start + "\nInterval = " + interval +
            "\nNode = " + node.node + "\tID = " + node.nodeId + "\n"
  }
}


// Create an actor for a peer
class Node(idc: Int, mc: Int) extends Actor {
  var nodeId = idc
  val m = mc
  val size = pow(2, m).toInt
  var successor: NodeInfo = null
  var predecessor: NodeInfo = null
  var finger:Array[fingertable] = new Array[fingertable](m + 1)
  var i = 0
  implicit val timeout = Timeout(30 seconds)
  var future:Future[Any]= null


  // ask node to find its predecessor
  def find_predecessor(id: Int): NodeInfo = {
  	var n1: NodeInfo = new NodeInfo(self, nodeId)
    var start = nodeId - 1
    var end = successor.nodeId
  	while(!belongs_to(id, start, end)) {
      if (n1.node != self) {
        future = n1.node ? ClosestPrecedingFinger(id)
        n1 = Await.result(future, timeout.duration).asInstanceOf[NodeInfo]
      }
      else
        n1 = closest_prec_finger(id)

      var n1Succ: NodeInfo = null
      if (n1.node != self) {
        future = n1.node ? GetSuccessor()
        n1Succ = Await.result(future, timeout.duration).asInstanceOf[NodeInfo]
      }
      else
        n1Succ = successor

  		start = n1.nodeId - 1
      end = n1Succ.nodeId
  	}

  	return n1;
  }

  // Hash function
  def hash(key: String): Int = {
    return MH3.stringHash(key)
  }

  def init_finger_table(n1: NodeInfo) = {
		if (n1.node != self) {
			future = n1.node ? FindSuccessor(finger(1).start)
			finger(1).node = Await.result(future, timeout.duration).asInstanceOf[NodeInfo]
		}
		else
			finger(1).node = successor

    successor = finger(1).node

		if (finger(1).node.node != self) {
			future = finger(1).node.node ? GetPredecessor()
			predecessor = Await.result(future, timeout.duration).asInstanceOf[NodeInfo]
		}

		if (successor.node != self) {
			future = successor.node ? SetPredecessor(new NodeInfo(self, nodeId))
			val success = Await.result(future, timeout.duration).asInstanceOf[Boolean]
		}
		else
			predecessor = new NodeInfo(self, nodeId)

    for(i <- 1 until m) {
      //var queryInterval = nodeId to (finger(i).node.nodeId - 1)
      val start = nodeId
      val end = finger(i).node.nodeId - 1
  		if (belongs_to(finger(i+1).start, start, end))
  			finger(i+1).node = finger(i).node
  		else {
				if (n1.node != self) {
					future = n1.node ? FindSuccessor(finger(i+1).start)
					finger(i+1).node = Await.result(future, timeout.duration).asInstanceOf[NodeInfo]
				}
				else
					finger(i+1).node = successor
  		}
  	}
  }

  def closest_prec_finger(id: Int): NodeInfo = {
      //val queryInterval = ((nodeId - 1) % pow(2, m)).toInt to ((id - 1) % pow(2, m)).toInt
      for (i <- m to 1 by -1) {
        if (belongs_to(nodeId, nodeId - 1, id - 1))
          return finger(i).node
      }
      return new NodeInfo(self, nodeId)
  }

  def update_others() = {
  	var p: NodeInfo = null
  	for(i <- 1 to m) {
      println("ITERATION " + i + " :Calling find_predecessor(" + ((nodeId - pow(2, i-1)) % size).toInt +")")
  		p = find_predecessor(((nodeId - pow(2, i-1)) % size).toInt)
      println(self + "\tAfter find_predecessor - " + p)
			if (p.node != null && p.node != self)
  			p.node ! UpdateFingerTable(new NodeInfo(self, nodeId), i)
			else
				update_finger_table(new NodeInfo(self, nodeId), i)
  	}
  }

	def update_finger_table(s: NodeInfo, i: Int): Unit = {
		//var queryInterval = nodeId to (finger(i).node.nodeId - 1)
		if (belongs_to(s.nodeId, nodeId, finger(i).node.nodeId - 1)) {
			finger(i).node = s
			val p = predecessor
			if (p.node != null && p.node != self)
				p.node ! UpdateFingerTable(s, i)
			else
				update_finger_table(s, i)
		}
	}

  def belongs_to(x: Int, int_start: Int, int_end: Int): Boolean = {
    if (int_start > int_end) {
      val qInt1 = int_start to size
      val qInt2 = size to int_end
      if (qInt1.contains(x) || qInt2.contains(x))
        return true
      else
        return false
    }
    else {
      var qInt = int_start to int_end
      return qInt.contains(x)
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
        finger(i) =  new fingertable(start, start, intEnd, new NodeInfo(self, nodeId));
      }

      for(i<-1 to m)
        print(finger(i).toString())

      if(n1 == null) {
        predecessor = new NodeInfo(self, nodeId)
        successor = new NodeInfo(self, nodeId)
      } else {
        init_finger_table(n1)
        println("inited " + self + " with help from " + n1.node)
        update_others()
        println("updated " + self + " with help from " + n1.node)
      }

      sender ! true

 	case GetNodeId() =>
    println("Let me get node id for " + sender)
 		sender ! nodeId

 	case GetSuccessor() =>
 		sender ! successor

 	case GetPredecessor() =>
 		sender ! predecessor

 	case SetPredecessor(pred: NodeInfo) =>
 		  predecessor = pred
      sender ! true

 	case FindSuccessor(id: Int) =>
    //var queryInterval = (nodeId-1) to successor.nodeId

    /*if (nodeId -1 == successor.nodeId)
      sender ! new NodeInfo(self, nodeId)*/

    var n1: NodeInfo = new NodeInfo(self, nodeId)
    var start = nodeId - 1
    var end = successor.nodeId
    while(belongs_to(id, start, end)) {
      if (n1.node != self) {
        future = n1.node ? ClosestPrecedingFinger(id)
        n1 = Await.result(future, timeout.duration).asInstanceOf[NodeInfo]
      }
      else
        n1 = closest_prec_finger(id)

      var n1Succ: NodeInfo = null
      if (n1.node != self) {
        future = n1.node ? GetSuccessor
        n1Succ = Await.result(future, timeout.duration).asInstanceOf[NodeInfo]
      }
      else
        n1Succ = successor

      start = n1.nodeId - 1
      end = n1Succ.nodeId
    }

    var succResult:NodeInfo = null
    if (n1.node != self) {
      future = n1.node ? GetSuccessor
      succResult = Await.result(future, timeout.duration).asInstanceOf[NodeInfo]
    }
    else
      succResult = finger(1).node

		sender ! succResult

 	case ClosestPrecedingFinger(id) =>
  		for (i <- m to 1 by -1) {
				if (belongs_to(finger(i).node.nodeId, nodeId - 1, id - 1)) {
  				sender ! finger(i).node
  			}
  		}
  		sender ! new NodeInfo(self, nodeId)

  case UpdateFingerTable(s: NodeInfo, i: Int) =>
    //var queryInterval = nodeId to (finger(i).node.nodeId - 1)
  	if (belongs_to(s.nodeId, nodeId, finger(i).node.nodeId - 1)) {
  		finger(i).node = s
  		var p = predecessor
  		p.node ? UpdateFingerTable(s, i)
  	}

    case GiveMeSomePlace(key) =>
      sender ! hash(key)

    case any =>
      println(self + " :ERROR: Unknown Message from " + sender)
      println(any)
  }
}


// Create the result listener
class TheArchitect extends Actor {
  def receive = {
    case Create(actors, nodeLocations, numRequests) =>
      implicit val timeout = Timeout(30 seconds)
      var future = actors(0) ? JoinNetwork(null)
      var success = Await.result(future, timeout.duration).asInstanceOf[Boolean]

      actors(nodeLocations(1)) ! JoinNetwork(new NodeInfo(actors(0), 0))
      Thread.sleep(30000)
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
