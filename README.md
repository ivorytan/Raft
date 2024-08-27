### Project1: Conflicts, Consistency, and Clocks

##### Description:
This project implements two approaches to achieve eventual and causal consistency in a distributed system. It is divided into two tasks:

Task 1: Physical Clocks
This task implements timestamp-based eventual consistency using physical clocks.

Task 2: Version Vectors
This task implements version-vector based causal consistency.

By completing the tasks, I gained a better understanding of how to achieve consistency in distributed systems and the trade-offs involved in choosing between different approaches.

##### Test cases:
1. physical_test.go
    - TestPhysicalConcurrentEventsHappenBefore: test the HappenBefore and Equals function
    - TestPhysicalClockString: test String function
    - TestPhysicalClockConflictResolver_ResolveConcurrentEvents: test ResolveConcurrentEvents function

2. vector_test.go
    - TestVectorConcurrentEventsDoNotHappenBefore: test the HappenBefore function
    - TestResolveConcurrentEvents: test ResolveConcurrentEvents function
    - TestVersionVectorConflictResolver_NewClock: test NewClock function

##### Any bugs:
No bugs in this project

##### Extra Features:
No extra features in this project



### Project2: Leaderless Replication

##### Description:
This project aims to implement a leaderless replication protocol for a distributed key-value store. The project is divided into three tasks, each implementing a particular aspect of the protocol:

Task 1: Helpers
In this task, two helper functions will be implemented: safelyUpdateKey and getUpToDateKV. The first function updates the key-value store if the incoming data is sufficiently up-to-date, while the second function returns the locally stored key-value pair for a given key as long as it is not less up-to-date than the specified clock.

Task 2: Write Path
In this task, we will implement the code responsible for handling write requests in a distributed setting. The task involves implementing three functions: HandlePeerWrite, replicateToNode, and ReplicateKey. HandlePeerWrite is an RPC endpoint that allows nodes to handle direct replication requests. replicateToNode dispatches a write request to another replica, while ReplicateKey replicates a specified put request to itself and W-1 other nodes.

Task 3: Read Path
In this task, we will implement the code responsible for handling read requests in a distributed setting. The task involves implementing four functions: HandlePeerRead, readFromNode, PerformReadRepair, and GetReplicatedKey. HandlePeerRead is an RPC endpoint that returns a local key-value pair for a given key. readFromNode is used by the coordinator of a read to ask another replica for its data. PerformReadRepair brings any lagging nodes up-to-date. Finally, GetReplicatedKey performs a quorum read (to itself and R-1 other nodes) and repairs replicas as necessary.

##### Test cases:
1. leaderless_test.go
    - TestBasicLeaderless: test task 1 and 2
    - TestBasicReadRepair: test task 3
    - TestReadNonexistentKeys: test none existent key situation, help find code bugs

##### Any bugs:
No bugs in the project

##### Extra Features:
1. Define a findMostUpToDateKV helper function to help find mostUpToDateKV in kvMap.


*Special thanks to TA John Liu for his invaluable help in debugging my code throughout this project.*


### Project3: Partitioning

##### Description:
This Partitioning project involves implementing a partitioner that uses consistent hashing to distribute keys across multiple replica groups. The partitioner will support the following tasks:

- Lookup: Given a key, the partitioner must determine which replica group it belongs to and return the hashed version of the key.

- AddReplicaGroup: This method adds a new replica group to the partitioner and determines which keys need to be immediately reassigned to it to satisfy the consistent hashing invariants.

- RemoveReplicaGroup: This method removes a replica group from the partitioner and determines to which other replica groups its keys need to be reassigned.

##### Test cases:
1. physical_test.go
    - TestAddReplicaGroupNoOp: test the case that replica group already exists
    - TestAddReplicaGroupFirstGroup: test the case that replica group is the first group
    - TestAddReplicaGroupSubsequentGroups: test the normal cases
    - TestRemoveReplicaGroupNotExist: test remove replica group not exist and the normal cases

##### Any bugs:
No bugs in this project

##### Extra Features:
No extra features in this project


### Project4: Tapestry

##### Description:
    Tapestry is a decentralized distributed object location and retrieval system (DOLR). Think of it as a peer-to-peer (P2P) network that utilizes simple prefix-based routing to store and retrieve objects across multiple computers. Each node serves as both an object store and a router that clients can contact to obtain objects. Put simply, objects are “published” at nodes, and once an object has been successfully published, it is possible for any other node in the network to find the location at which that object is published.

    In the previous project, we introduced the notion of multiple replica groups. With the help of Tapestry, we will now implement a distributed hash table (DHT) to store a mapping of replica group IDs to their TCP addresses. 


##### Test cases:
1. sample_test.go
    - TestSampleTapestrySetup: This test case sets up a sample Tapestry network with 4 nodes, kills 2 of them, and then tests if a key lookup is routed correctly after the node kills. It checks the correctness of node killing functionality.

    - TestSampleTapestrySearch: This test case sets up a sample Tapestry network with 3 nodes, stores a key-value pair in one of the nodes, and then tests if the stored value can be retrieved correctly. It checks the correctness of key-value storage and retrieval functionality.

    - TestSampleTapestryAddNodes: This test case sets up a sample Tapestry network with 3 initial nodes, adds 2 additional nodes to the network, and then tests if a key lookup is routed correctly after the node additions. It checks the correctness of node addition functionality.

    - TestSampleTapestryLeaveAndNotifyLeave: This test case sets up a sample Tapestry network with 4 nodes, simulates a node leaving the network, and then tests if a key lookup is routed correctly after the node leaves. It checks the correctness of node leaving and key routing functionality.

    - TestAddFunction: This test case tests the Add function of the RoutingTable data structure. It fills up all the slots in the routing table and then attempts to add a remote node to a slot that is already full. It checks the correctness of the Add function in handling a full routing table.

    - TestSampleTapestryAddRoute: This test case sets up a sample Tapestry network with 1 node, fills up all the slots in the routing table of the node, and then attempts to add a new route to the routing table. It checks the correctness of adding a new route to a full routing table.

##### Any bugs:
No bugs in this project

##### Extra Features:
No extra features in this project