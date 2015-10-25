import akka.actor._
import akka.actor.Props
import com.sun.org.apache.xalan.internal.lib
import scala.concurrent.duration.Duration
import com.typesafe.config.ConfigFactory
import scala.math._
import scala.util.control._
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
case class SetSuccessor(succ: NodeInfo) extends chord
case class GetPredecessor() extends chord
case class SetPredecessor(pred: NodeInfo) extends chord
case class FindSuccessor(id: Int) extends chord
case class ClosestPrecedingFinger(id: Int) extends chord
case class UpdateFingerTable(s: NodeInfo, i: Int) extends chord
case class GiveMeSomePlace(ip: String) extends chord
case class StartQuerying() extends chord
case class Result(node: Int) extends chord
case class Lookup(initiator: ActorRef, hopcount: Int, node: Int)
case class LicenseToKill(nodesRef: Array[ActorRef], kill: Boolean) extends chord
case class Kill() extends chord
case class ICER(timeToSleep: Int) extends chord
case class Threaten() extends chord

class NodeInfo (n: ActorRef, id: Int) {
  var node: ActorRef = n
  var nodeId: Int = id

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

// Contract Killer
class ContractKiller(nodesToKill: Int) extends Actor {

  def receive = {
    case LicenseToKill(nodesRef, kill) =>
      for(i <- 0 until nodesToKill) {
        if(kill) {
          nodesRef(i) ! Kill()
        } else {
          nodesRef(i) ! ICER(10)
        }
      }

    case _ =>
      println("Error: Invalid message!")
      System.exit(1)
  }
}

// Create an actor for a peer
class Node(idc: Int, mc: Int, numreqc: Int) extends Actor {
  var nodeId = idc
  val m = mc
  val numreq = numreqc
  val size = (pow(2, m)).toInt
  var successor: NodeInfo = null
  var predecessor: NodeInfo = null
  var finger:Array[fingertable] = new Array[fingertable](m + 1)
  var i = 0
  var currentState = "ALIVE"
  implicit val timeout = Timeout(3000 seconds)
  var future:Future[Any]= null


  // ask node to find its predecessor
  def find_predecessor(id: Int): NodeInfo = {
    var n1: NodeInfo = new NodeInfo(self, nodeId)
    var start = (nodeId + 1) % size
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

      if (n1.node == self)
        return n1

      start = (n1.nodeId + 1) % size
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

    // predecessor = successor.predecessor
    if (finger(1).node.node != self) {
      future = finger(1).node.node ? GetPredecessor()
      predecessor = Await.result(future, timeout.duration).asInstanceOf[NodeInfo]
    }
    else
      predecessor = new NodeInfo(self, nodeId)

    // successor.predecessor = n
    if (successor.node != self) {
      future = successor.node ? SetPredecessor(new NodeInfo(self, nodeId))
      val success = Await.result(future, timeout.duration).asInstanceOf[Boolean]
    }
    else
      predecessor = new NodeInfo(self, nodeId)

    // Set predecessor.successor = n
    if (predecessor.node != self) {
      future = predecessor.node ? SetSuccessor(new NodeInfo(self, nodeId))
      val success = Await.result(future, timeout.duration).asInstanceOf[Boolean]
    }
    else
      successor = new NodeInfo(self, nodeId)

    for(i <- 1 until m) {
      //var queryInterval = nodeId to (finger(i).node.nodeId - 1)

      var end = finger(i).node.nodeId - 1
      if (end < 0)
        end += size

      if (belongs_to(finger(i+1).start, nodeId, end))
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
    var end = -1
    for (i <- m to 1 by -1) {
      end = id - 1
      if (end < 0)
        end += size

      //println("Checking finger " + i + " : " + nodeId + " : Comparing " + finger(i).node.nodeId + " with (" + (nodeId + 1) + ", " + (id - 1) + ")")
      if (belongs_to(finger(i).node.nodeId, (nodeId + 1) % size, end))
        return finger(i).node
    }
    return new NodeInfo(self, nodeId)
  }

  def update_others() = {
    var p: NodeInfo = null
    var pred_id = -1
    for(i <- 1 to m) {
      pred_id = ((nodeId - pow(2, i-1)) % size).toInt
      if (pred_id < 0)
        pred_id += size

      //println("ITERATION " + i + " :Calling find_predecessor(" + pred_id +")")
      p = find_predecessor(pred_id)
      if (p.node != self) {
        //println(self + "\tUpdating finger entry " + i + " for " + p)
        p.node ? UpdateFingerTable(new NodeInfo(self, nodeId), i)
        //var success = Await.Result(future, timeout.duration).asInstanceOf[]
      }
      else {
        //println(self + "\tUpdating finger entry " + i + " for " + self)
        update_finger_table(new NodeInfo(self, nodeId), i)
      }
    }
  }

  def update_finger_table(s: NodeInfo, i: Int): Unit = {
    //var queryInterval = nodeId to (finger(i).node.nodeId + 1)
    var end = finger(i).node.nodeId - 1
    if (end < 0)
      end += size

    if (belongs_to(s.nodeId, nodeId, end)) {
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
      val qInt1 = int_start to (size - 1)
      val qInt2 = 0 to int_end
      if (qInt1.contains(x) || qInt2.contains(x))
        return true
      else
        return false
    }
    else {
      val qInt = int_start to int_end
      return qInt.contains(x)
    }
  }

  def receive = {
    case Initialize(actors) =>
      successor = finger(1).node
      for(i <- 1 to m)
        println("Actor = " + self + " ID: " + nodeId + "\tfinger["+i+"] "+ finger(i).toString()) 

    case JoinNetwork(n1) =>
      //println("Node " + nodeId + " joining the network")

      //Create the finger table
      for(i <- 1 to m) {
        val start = ((nodeId + pow(2, (i-1))) % size).toInt
        val intEnd = ((nodeId + pow(2, i)) % size).toInt
        finger(i) =  new fingertable(start, start, intEnd, new NodeInfo(self, nodeId));
      }

      /*      for(i<-1 to m)
      print(finger(i).toString())*/

     if(n1 == null) {
       predecessor = new NodeInfo(self, nodeId)
       successor = new NodeInfo(self, nodeId)
     } else {
       init_finger_table(n1)
       //println("inited " + self + " with help from " + n1.node)
       /*for(i<-1 to m)
       println(self + finger(i).toString())*/

      update_others()
      //println("updated " + self + " with help from " + n1.node)
     }
     sender ! true

    case GetNodeId() =>
      println("Let me get node id for " + sender)
      sender ! nodeId

    case GetSuccessor() =>
      sender ! successor

    case SetSuccessor(succ: NodeInfo) =>
      successor = succ
      sender ! true

    case GetPredecessor() =>
      sender ! predecessor

    case SetPredecessor(pred: NodeInfo) =>
      predecessor = pred
      sender ! true

    case FindSuccessor(id: Int) =>
      //var queryInterval = (nodeId-1) to successor.nodeId

      var n1: NodeInfo = new NodeInfo(self, nodeId)

      // "id" belongs to (n1, n1.successor]
      var flag = true
      if (n1.node == successor.node)
        flag = false

      var start = (nodeId + 1) % size
      var end = successor.nodeId
      val loop = new Breaks
      loop.breakable {
        while (!belongs_to(id, start, end) && flag) {
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

          if (n1.node == successor.node) {
            loop.break
          }

          start = (n1.nodeId + 1) % size
          end = n1Succ.nodeId
        }
      }

      var succResult:NodeInfo = null
      if (n1.node != self) {
        future = n1.node ? GetSuccessor()
        succResult = Await.result(future, timeout.duration).asInstanceOf[NodeInfo]
      }
      else
        succResult = finger(1).node

      sender ! succResult

    case ClosestPrecedingFinger(id) =>
      val n = closest_prec_finger(id)
      sender ! n
      /*for (i <- m to 1 by -1) {
        if (belongs_to(finger(i).node.nodeId, nodeId + 1, id + 1)) {
          sender ! finger(i).node
        }
      }
      sender ! new NodeInfo(self, nodeId)*/

    case UpdateFingerTable(s: NodeInfo, i: Int) =>
      //var queryInterval = nodeId to (finger(i).node.nodeId - 1)
      var end = finger(i).node.nodeId - 1
      if (end < 0)
        end += size

      if (belongs_to(s.nodeId, nodeId, end)) {
        finger(i).node = s
        predecessor.node ? UpdateFingerTable(s, i)
      }

    case GiveMeSomePlace(key) =>
      sender ! hash(key)

    case StartQuerying =>
      if(currentState == "ALIVE") {
        var randomstring = ""
        var hash = 0
        for(i <- 0 until numreq) {
          //Create a random string
          randomstring = scala.util.Random.alphanumeric.take(15).mkString
          //Hash the random string and mod it by size of network
          hash = scala.math.abs(MH3.stringHash(randomstring) % size)
          //Search for that Node
          finger(1).node.node ! Lookup(self, 1, hash)
          //println("Start Querying Received in " + self + " for " + hash)
          Thread sleep(1000)
        }
      }

    case Result(numHops) =>
      if(currentState == "ALIVE") {
      println(self + " found the node in " + numHops + " hops")
      }

    case Lookup(initiator, hopcount, lookup_node) =>
      if(currentState == "ALIVE") {
        //println(self + " : " + lookup_node)
        if(belongs_to(lookup_node, (predecessor.nodeId+1)%size, nodeId)) {
          initiator ! Result(hopcount)
        } else {
          for(i<- m to 1 by -1) {
            var end = finger(i).interval._2 - 1
            if (end < 0)
              end += size

            if(belongs_to(lookup_node, finger(i).interval._1, end)) {
              finger(i).node.node ! Lookup(initiator, hopcount+1, lookup_node)
            }
          }
        }
      }
    case Kill() =>
      currentState = "DEAD"
    case ICER(time) =>
      Thread sleep(time)
    case Threaten() =>
        println("Node " +nodeId +" leaving the network")
    case any =>
      println(self + " :ERROR: Unknown Message from " + sender)
      println(any)
  }
}


// Create the result listener
class TheArchitect extends Actor {
  def receive = {
    case Create(actors, nodeLocations, numRequests) =>
      implicit val timeout = Timeout(3000 seconds)
      var future = actors(0) ? JoinNetwork(null)
      var success = Await.result(future, timeout.duration).asInstanceOf[Boolean]

      if (!success) {
        println("Problem creating the network topology. Exiting...")
        System.exit(1)
      }


      //actors(nodeLocations(1)) ! JoinNetwork(new NodeInfo(actors(0), 0))
      //Thread.sleep(30000)
      for(i <- 1 until nodeLocations.length) {
        //actors(nodeLocations(i)) ! JoinNetwork(new NodeInfo(actors(0), nodeLocations(0)))
        future = actors(nodeLocations(i)) ? JoinNetwork(new NodeInfo(actors(0), nodeLocations(0)))
        success = Await.result(future, timeout.duration).asInstanceOf[Boolean]
        //println(i)
      }

      for(i <- 0 until nodeLocations.length) {
        actors(nodeLocations(i)) ! StartQuerying
      }


      //Create a killer to implement failure
      var killer = context.actorOf(Props(new ContractKiller(1)))
      killer ! LicenseToKill(actors, true)


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
        PeerNodes(i) = system.actorOf(Props(new Node(i, m, numRequests)))
        NodeLocations(idx) = i
        //println(PeerNodes(i) + " present at " + i)
        idx += 1
      }
    }

    val Arch = system.actorOf(Props[TheArchitect])
    Arch ! Create(PeerNodes, NodeLocations, numRequests)

    println("\n")

  }
}
