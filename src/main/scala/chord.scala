import akka.actor._
import akka.actor.Props
import akka.routing.RoundRobinRouter
import scala.concurrent.duration.Duration
import com.typesafe.config.ConfigFactory
import java.security.MessageDigest
import java.io.File


// Create an actor for a peer
class Peer() extends Actor {

  def receive = {
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
