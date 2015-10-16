name := "My Project"
version := "1.0"
scalaVersion := "2.11.7"
libraryDependencies ++= Seq( 
 "com.typesafe.akka" % "akka-actor_2.11" % "2.3.4",
 "com.typesafe.akka" % "akka-remote_2.11" % "2.3.4" exclude("com.typesafe.akka", "akka-remote_2.10"))
resolvers += "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"
resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
