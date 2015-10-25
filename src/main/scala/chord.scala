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

case class Create(actors: Array[ActorRef], nodeLocations:Array[Int], numRequests: Int) extends chord
case class JoinNetwork(n: NodeInfo) extends chord
case class SendRequest(requesterRef: ActorRef, message: String, hopNum: Int) extends chord
case class GetSuccessor() extends chord
case class SetSuccessor(succ: NodeInfo) extends chord
case class GetPredecessor() extends chord
case class SetPredecessor(pred: NodeInfo) extends chord
case class FindSuccessor(id: Int) extends chord
case class ClosestPrecedingFinger(id: Int) extends chord
case class UpdateFingerTable(s: NodeInfo, i: Int) extends chord
case class GiveMeSomePlace(ip: String) extends chord
case class StartQuerying() extends chord
case class Result(node: Int, message: String) extends chord
case class Lookup(initiator: NodeInfo, hopcount: Int, node: Int) extends chord
case class RespondWithResult(initiator: NodeInfo, hopcount: Int, node:Int) extends chord
case class FoundMessage(hops: Int) extends chord

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


// Create an actor for a peer
class Node(idc: Int, mc: Int, numreqc: Int, message: String, tank: ActorRef) extends Actor {
  var nodeId = idc
  val m = mc
  val numreq = numreqc
  val size = pow(2, m).toInt
  var successor: NodeInfo = null
  var predecessor: NodeInfo = null
  var finger:Array[fingertable] = new Array[fingertable](m + 1)
  var i = 0
  implicit val timeout = Timeout(500 seconds)
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

    return n1
  }

  // Hash function
  def hash(key: String): Int = {
    return MH3.stringHash(key)
  }

  def init_finger_table(n1: NodeInfo) = {
    //println("\ninit_finger_table for " + nodeId)
    if (n1.node != self) {
      future = n1.node ? FindSuccessor(finger(1).start)
      finger(1).node = Await.result(future, timeout.duration).asInstanceOf[NodeInfo]
    }
    else
      finger(1).node = successor

    successor = finger(1).node
    //println("Succ Returned = " + successor.nodeId)

    // predecessor = successor.predecessor
    future = successor.node ? GetPredecessor()
    predecessor = Await.result(future, timeout.duration).asInstanceOf[NodeInfo]
    //println("Pred returned = " + predecessor.nodeId)

    future = predecessor.node ? SetSuccessor(new NodeInfo(self, nodeId))
    var success = Await.result(future, timeout.duration).asInstanceOf[Boolean]

    future = successor.node ? SetPredecessor(new NodeInfo(self, nodeId))
    success = Await.result(future, timeout.duration).asInstanceOf[Boolean]

       //println(predecessor.nodeId + ".succ = " + nodeId)

       // println("\nNODE " + nodeId + "\tPred = " + predecessor.nodeId + "  Succ = " + successor.nodeId)

       for(i <- 1 until m) {
         var end = finger(i).node.nodeId - 1
         if (end < 0)
           end += size

         if (belongs_to(finger(i+1).start, nodeId, end))
           finger(i+1).node = finger(i).node
         else {
           if (n1.node != self) {
             //println("\n#### Calling " + nodeId + ".finger(" + (i+1) + ").start = " + finger(i+1).start)
             future = n1.node ? FindSuccessor(finger(i+1).start)
             finger(i+1).node = Await.result(future, timeout.duration).asInstanceOf[NodeInfo]
           }
           else
             finger(i+1).node = successor
         }
       }
  }

  def closest_prec_finger(id: Int): NodeInfo = {
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
      pred_id = (nodeId - pow(2, i-1)).toInt
      if (pred_id < 0)
        pred_id += size

      //println("\nITERATION " + i + " :Calling find_predecessor(" + pred_id +")")
      p = find_predecessor(pred_id)
      p.node ? UpdateFingerTable(new NodeInfo(self, nodeId), i)
  }
}

def update_finger_table(s: NodeInfo, i: Int): Unit = {
  var end = finger(i).node.nodeId - 1
  if (end < 0)
    end += size

  if (belongs_to(s.nodeId, nodeId, end)) {
    //println(nodeId + "\tUpdating " + nodeId + ".finger(" + i + ") = " + s.nodeId)
    finger(i).node = s
    val p = predecessor
    p.node ? UpdateFingerTable(s, i)
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

  case JoinNetwork(n1) =>
    //println("Node " + nodeId + " joining the network")

    //Create the finger table
    for(i <- 1 to m) {
      val start = ((nodeId + pow(2, (i-1))) % size).toInt
      val intEnd = ((nodeId + pow(2, i)) % size).toInt
      finger(i) =  new fingertable(start, start, intEnd, new NodeInfo(self, nodeId));
    }

   if(n1 == null) {
     predecessor = new NodeInfo(self, nodeId)
     successor = new NodeInfo(self, nodeId)
   } else {
     init_finger_table(n1)
    update_others()
   }
   sender ! self

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
        //println(nodeId + " id = " + id + " start = " + start + "  end = "  + end)
        if (n1.node != self) {
          future = n1.node ? ClosestPrecedingFinger(id)
          n1 = Await.result(future, timeout.duration).asInstanceOf[NodeInfo]
        }
        else
          n1 = closest_prec_finger(id)

        //println("CPF = " + n1)

        var n1Succ: NodeInfo = null
        if (n1.node != self) {
          //println(n1.nodeId + " calling GetSucc()")
          future = n1.node ? GetSuccessor()
          n1Succ = Await.result(future, timeout.duration).asInstanceOf[NodeInfo]
          //println(n1.nodeId + " Got Succ = " + n1Succ.nodeId)
        }
        else
          n1Succ = successor

        if (n1.node == self) {
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


  case UpdateFingerTable(s: NodeInfo, i: Int) =>
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

    var randomstring = ""
    var hash = 0
    for(i <- 0 until numreq) {
      //Create a random string
      randomstring = scala.util.Random.alphanumeric.take(15).mkString
      //Hash the random string and mod it by size of network
      hash = scala.math.abs(MH3.stringHash(randomstring) % size)

      println("Start Querying Received in Node " + nodeId + " for " + hash)

       val f = closest_prec_finger(hash)
       f.node ! Lookup(new NodeInfo(self, nodeId), 0, hash)

       // Actors should send a message every 1 sec
       Thread sleep(1000)
       }

  case Result(numHops, message) =>
    println(nodeId + " found message : " + message + " in " + numHops + " hops - initiated by " + sender)
    tank ! FoundMessage(numHops)

  case Lookup(initiator, hopcount, lookup_node) =>
    var end = nodeId - 1
    if (end < 0)
      end += size

    if(belongs_to(lookup_node, (predecessor.nodeId + 1) % size, nodeId)) {
      initiator.node ! Result(hopcount+1, message)
    } else {

      if(hopcount > ceil(log(m)/log(2)).toInt) {
        successor.node ! Lookup(initiator, hopcount + 1, lookup_node)
      }
      else {

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

  case any =>
    println(self + " :ERROR: Unknown Message from " + sender)
    println(any)
    }
}


// Create the result listener
class Tank(totalMessagesc: Int) extends Actor {

  var totalMessages = totalMessagesc
  var messagesReceived = 0
  var totalHops = 0

  def receive = {
    case FoundMessage(hops) =>
      messagesReceived  += 1
      totalHops += hops
      if(messagesReceived == totalMessages) {
        println("Average Number of Hops = " + totalHops/messagesReceived)
        System.exit(0)
      }
    case _ => println("INVALID MESSAGE")
    System.exit(1)
  }
}

// Create the Architect
class TheArchitect extends Actor {
  def receive = {
    case Create(actors, nodeLocations, numRequests) =>
      implicit val timeout = Timeout(500 seconds)
      var future = actors(0) ? JoinNetwork(null)
      var success = Await.result(future, timeout.duration).asInstanceOf[ActorRef]

      if (success == null) {
        println("Problem creating the network topology. Exiting...")
        System.exit(1)
      }


      for(i <- 1 until nodeLocations.length) {
        future = actors(nodeLocations(i)) ? JoinNetwork(new NodeInfo(actors(0), nodeLocations(0)))
        success = Await.result(future, timeout.duration).asInstanceOf[ActorRef]
      }

      println("Finished Creating the Network")
      for(i <- 0 until nodeLocations.length) {
        actors(nodeLocations(i)) ! StartQuerying
      }


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
    val m = 20
    val size = pow(2, m).toInt

    // Number of requests each peer will make
    var numRequests = args(1).toInt

    val system = ActorSystem("Chord")
    var PeerNodes:Array[ActorRef] = new Array[ActorRef](size)
    var NodeLocations:Array[Int] = new Array[Int](numNodes)
    var idx = 0

    //Create the listener
    val tank = system.actorOf(Props(new Tank(numNodes * numRequests)), name = "listener")

    // Initialize the actor nodes to NULL
    for( i <- 0 until size) {
      PeerNodes(i) = null
    }

    val slots = floor(size/numNodes).toInt
    println("Peer Network of size " + size)

    var msg = ""
    for( i <- 0 until size by slots) {
      if (idx < numNodes) {
        msg = "Message" + i
        PeerNodes(i) = system.actorOf(Props(new Node(i, m, numRequests, msg, tank)))
        NodeLocations(idx) = i
        idx += 1
      }
    }


    //Create the Architect
    val Arch = system.actorOf(Props[TheArchitect])
    Arch ! Create(PeerNodes, NodeLocations, numRequests)

    println("\n")

  }
}