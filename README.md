# Team
Rahul Prabhu

Sanil Sinai Borkar


# Compiling
SBT is used to build the code. To compile, just run the following command from the project root direcory
```
$ sbt compile
```

# Running
To run the program, type 'sbt' at the command prompt to enter the 'sbt' command-prompt.
```
$ sbt
```

The program then runs with the following command:
```
$ run <numNodes> <numRequests>
```

Here, **numNodes** denotes the number of nodes in the P2P network, **numRequests** denotes the number of requests each node has to make.


# Command Line Arguments
There are 2 command-line arguments need to be input to the program:

* **numNodes** - The number of nodes in the Chord P2P network. It is a positive integer value.
* **numRequests** - The number of requests that each peer node has to make. One such request will be made per second.

sbt will prompt the main class to select. Please select ***edu.zeroday.chord.Chord***.

Any other values will result in an error, and program termination.


# Working
Chord works on the principle of keeping a track of its neighbors, in its finger tables, which are located at distances of powers of 2 from itself. When searching for a query it makes use of its finger tables to find the closest preceding node to the query being searched, and this node will then do the same.

We have selected m = 20. Therefore, the maximum network size is 2<sup>m</sup> = 1048576, with node IDs lying in the range [0, 1048576).

Since we are not making use of concurrent node joins, the nodes join the network synchronously. Moreover, once a new node joins, it may need to update the finger tables of its predecessors. Since this update is done synchronously, the network takes time to build up before we can start querying.

# Performance Measure
To measure the performance viz. the average number of hops required to deliver a message (or perform a query lookup) was measured.


## Average Number of Hops
| Network Size | Average Number of Hops |
| ------------ | ---------------------- |
| 10 | 3 |
| 100 | 9 |
| 1000 | 12 |
| 10000 | 18 |
| 50000 | 26 |

Most of the execution time was spent on creating the network and updating the finger tables of all the nodes. Lookup took very less time.

The program was run on a 4-core machine.