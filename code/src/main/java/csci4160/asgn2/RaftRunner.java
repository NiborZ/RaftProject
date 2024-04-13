package csci4160.asgn2;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import raft.Raft.AppendEntriesArgs;
import raft.Raft.AppendEntriesReply;
import raft.Raft.CheckEventsArgs;
import raft.Raft.CheckEventsReply;
import raft.Raft.GetValueArgs;
import raft.Raft.GetValueReply;
import raft.Raft.LogEntry;
import raft.Raft.ProposeArgs;
import raft.Raft.ProposeReply;
import raft.Raft.RequestVoteArgs;
import raft.Raft.RequestVoteReply;
import raft.Raft.SetElectionTimeoutArgs;
import raft.Raft.SetElectionTimeoutReply;
import raft.Raft.SetHeartBeatIntervalArgs;
import raft.Raft.SetHeartBeatIntervalReply;
import raft.RaftNodeGrpc;
import raft.RaftNodeGrpc.RaftNodeBlockingStub;

//MY IMPORT
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.*;
import java.lang.*;
import raft.Raft.Operation;
import raft.Raft.Status;

public class RaftRunner {

  public enum ServerState
  {
    FOLLOWER, CANDIDATE, LEADER
  }

  public static void main(String[] args) throws Exception 
  {
    String ports = args[1];
    int myport = Integer.parseInt(args[0]);
    int nodeID = Integer.parseInt(args[2]);
    int heartBeatInterval = Integer.parseInt(args[3]);
    int electionTimeout = Integer.parseInt(args[4]);

    String[] portStrings = ports.split(",");

    // A map where
    //    the key is the node id
    //    the value is the {hostname:port}
    Map<Integer, Integer> hostmap = new HashMap<>();
    for (int x = 0; x < portStrings.length; x++) {
      hostmap.put(x, Integer.valueOf(portStrings[x]));
    }

    RaftNode node = NewRaftNode(myport, hostmap, nodeID, heartBeatInterval, electionTimeout);

    final Server server = node.getGrpcServer();
    //Stop the server
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        server.shutdown();
        System.err.println("*** server shut down");
      }
    });

    server.awaitTermination();

  }

  // Desc:
  // NewRaftNode creates a new RaftNode. This function should return only when
  // all nodes have joined the ring, and should return a non-nil error if this node
  // could not be started in spite of dialing any other nodes.
  //
  // Params:
  // myport: the port of this new node. We use tcp in this project.
  //          Note: Please listen to this port rather than nodeidPortMap[nodeId]
  // nodeidPortMap: a map from all node IDs to their ports.
  // nodeId: the id of this node
  // heartBeatInterval: the Heart Beat Interval when this node becomes leader. In millisecond.
  // electionTimeout: The election timeout for this node. In millisecond.
  public static RaftNode NewRaftNode(int myPort, Map<Integer, Integer> nodeidPortMap, int nodeId, int heartBeatInterval,
    int electionTimeout) throws IOException {
        //TODO: implement this !
        int numNodes = nodeidPortMap.size();

        nodeidPortMap.remove(nodeId);

        Map<Integer, RaftNodeBlockingStub> hostConnectionMap = new HashMap<>();

        RaftNode raftNode = new RaftNode();

        Server server = ServerBuilder.forPort(myPort).addService(raftNode).build();
        raftNode.server = server;
        server.start();

        //create channel to other RaftNode
        for (Map.Entry<Integer, Integer> entry : nodeidPortMap.entrySet()) {
          int id = entry.getValue();
          Channel channel = ManagedChannelBuilder.forAddress("127.0.0.1", id)
              .usePlaintext() // disable TLS
              .build();

          hostConnectionMap.put(
              id,
              RaftNodeGrpc.newBlockingStub(channel)
          );
        }

        System.out.println("Successfully connect all nodes");

        raftNode.nodeId = nodeId;
        raftNode.hostConnectionMap = hostConnectionMap;
        raftNode.nodeidPortMap = nodeidPortMap;        
        raftNode.numNodes = numNodes;
        raftNode.electionTimeout = electionTimeout;
        raftNode.heartBeatInterval = heartBeatInterval; 
        raftNode.nextIndex = new int[numNodes];
        for (int i = 0; i < numNodes; i++) {
          raftNode.nextIndex[i] = 0;
        }
        raftNode.lastTerm = new int[numNodes];
        for (int i = 0; i < numNodes; i++) {
          raftNode.lastTerm[i] = 0;
        }

        raftNode.timer = new Timer();
        raftNode.mainIteration();

        return raftNode;
    }


  public static class RaftNode extends RaftNodeGrpc.RaftNodeImplBase {

    public Server server;
    public List<LogEntry> log;
    public List<LogEntry> currentLog;
    //TODO: add class attributes you needed
    public int currentLeader;
    public int electionTimeout;
    public int heartBeatInterval;
    public int currentTerm;
    public int votedFor;
    public int commitIndex;
    public int leaderCommitIndex;
    public int[] nextIndex;
    public int[] lastTerm;
    public List<Integer> matchIndex;
    public Map<String,Integer> keyValueMap;
    public ServerState serverState;
    public boolean grantedVote;
    public boolean getHeartBeatSignal;
    public int prevLogIndex;
    public int prevLogTerm;

    public int nodeId;
    public int numNodes;
    public Map<Integer, Integer> nodeidPortMap;
    public Map<Integer, RaftNodeBlockingStub> hostConnectionMap;

    public Timer timer;
    public Timer leaderTimer;
    public FollowerTask followerTask;
    public LeaderTask leaderTask;

    public boolean electionTimeChangeFinished = true;
    public String testString = "";
    public int waiting = 0;

    public List<Object> lockList;
    public List<Integer> lockCounterList;
    public int lockIndex = 0;    
    // public Object lock = new Object();


    public RaftNode(){
      //TODO: Implement this!
      currentTerm = 0;
      serverState = ServerState.FOLLOWER;
      commitIndex = 0;
      leaderCommitIndex = 0;
      grantedVote = false;
      log = new ArrayList<LogEntry>();
      currentLog = new ArrayList<LogEntry>();
      matchIndex = new ArrayList<Integer>();
      matchIndex.add(0);
      keyValueMap = new HashMap<String, Integer>();
      lockList = new ArrayList<Object>();
      lockCounterList = new ArrayList<Integer>();
    }

    public void mainIteration(){
      if(!serverState.equals(ServerState.LEADER))
      {
        followerTask = new FollowerTask();
        timer.schedule(followerTask, electionTimeout, electionTimeout);
      }
      else if(serverState.equals(ServerState.LEADER))
      {
        // timer = new Timer();
        leaderTask = new LeaderTask();
        leaderTimer.schedule(leaderTask, 0, heartBeatInterval);
      }
    }
    
    class FollowerTask extends TimerTask {
      public void run() {
        //If not heard from leader
        // if(nodeId == 0)
        // {
        //   System.out.println("Node " + nodeId);
        //   System.out.println("getHeartBeatSignal: " + getHeartBeatSignal);
        //   System.out.println("serverState: " + serverState);
        //   System.out.println("grantedVote: " + grantedVote);
        //   System.out.println("electionTimeout: " + electionTimeout);
        //   System.out.println("heartBeatInterval: " + heartBeatInterval);
        // }
        if(!getHeartBeatSignal && !serverState.equals(ServerState.LEADER) && !grantedVote) 
        {
          // Start election
          // Term + 1
          currentTerm++;
          System.out.println(nodeId + " Start an election: " + currentTerm+ " time: "+System.currentTimeMillis());
          // Change to Candidate
          serverState = ServerState.CANDIDATE;
          // Vote self
          int from = nodeId;
          int to = nodeId;
          int term = currentTerm;
          int candidateId = nodeId;
          int lastLogIndex = 0;// Not sure
          int lastLogTerm = 0;// Not sure

          int respondCount = 1; // Self vote added
          grantedVote = true;
          // Send RequestVote to others
          ExecutorService pools = Executors.newFixedThreadPool(numNodes);
          List<Future<RequestVoteReply>> list = new ArrayList<Future<RequestVoteReply>>();

          for (Map.Entry<Integer, Integer> entry : nodeidPortMap.entrySet())
          {
            int otherNodeId = entry.getKey();
            RaftNodeBlockingStub stub = hostConnectionMap.get(entry.getValue());

            RequestVoteArgs requestVoteArgs = RequestVoteArgs.newBuilder()
              .setFrom(from)
              .setTo(otherNodeId)
              .setTerm(term)
              .setCandidateId(candidateId)
              .setLastLogIndex(lastLogIndex)
              .setLastLogTerm(lastLogTerm)
              .build();

            // RequestVoteReply respondRequestVote = stub.requestVote(requestVoteArgs);
            Future<RequestVoteReply> futures = pools.submit(new RequestVoteTask(requestVoteArgs, stub));
            list.add(futures);            
          }
          for(Future<RequestVoteReply> f : list){
            try {
              // RequestVoteReply respondRequestVote = f.get(1000, TimeUnit.MILLISECONDS);
              RequestVoteReply respondRequestVote = f.get(electionTimeout/(numNodes), TimeUnit.MILLISECONDS);
              if(respondRequestVote != null)
              {
                if(respondRequestVote.getVoteGranted())
                {
                  respondCount++;
                }
                // WIN ELECTION and not heard from others
                if(serverState.equals(ServerState.CANDIDATE) && (respondCount > numNodes / 2) && !getHeartBeatSignal)
                {
                  // SEND HEARTBEAT: As leader, send heart beat to all servers
                  serverState = ServerState.LEADER;
                  currentLeader = nodeId;
                  System.out.println(nodeId + " becomes leader: "+scheduledExecutionTime());
                  leaderTimer = new Timer();
                  mainIteration();
                  timer.cancel();
                  System.out.println(nodeId + " canceled follower iteration: "+scheduledExecutionTime());              
                }
                // Others is already leader, get heartbeat when 
                else if(getHeartBeatSignal)
                {
                  // Back to a follower
                  serverState = ServerState.FOLLOWER;
                  getHeartBeatSignal = false;
                  System.out.println(nodeId + " in election but get heartbeat from leader: "+scheduledExecutionTime());
                  break;
                }
              }
            } catch (InterruptedException e) {
              System.out.println("InterruptedException");
              e.printStackTrace();
            } catch (ExecutionException e) {
              System.out.println("ExecutionException");
              e.printStackTrace();
            } catch (TimeoutException e) {
              System.out.println(nodeId + " TimeoutException in sending to: " + list.indexOf(f));
              // serverState = ServerState.FOLLOWER;
              e.printStackTrace();
            }
          }
          serverState = ServerState.FOLLOWER;
          grantedVote = false;
          // System.out.println(nodeId + " Stoped this election: " + currentTerm + " time: "+System.currentTimeMillis());
          pools.shutdown();
        }
        // Get heartbeat, nothing happend
        else
        {
          serverState = ServerState.FOLLOWER;
          System.out.println(nodeId + " cannot become candidate: "+scheduledExecutionTime());
          getHeartBeatSignal = false;
        }
      }
    }

    class LeaderTask extends TimerTask {
      public void run() {
        // Heartbeat info
        boolean isHeartbeat = true;
        int apFrom = nodeId;
        int apTerm = currentTerm;
        int apLeaderId = nodeId;
        int apLeaderCommit = leaderCommitIndex; // Not Sure
        // System.out.println("testString1: " + testString + waiting);
        // for (Map.Entry<String, Integer> entry : keyValueMap.entrySet()) {
        //     System.out.println(entry.getKey() + ":" + entry.getValue().toString());
        // }
        // System.out.println("currentLog.size(): " + currentLog.size()+" "+System.currentTimeMillis());
        // if(currentLog.size()!=0)
        // {
        //   // isHeartbeat = false;
        //   LogEntry testEntry = currentLog.get(0);
        //   System.out.println("currentlog.key: " + testEntry.getKey());
        //   System.out.println("currentlog.value: " + testEntry.getValue());
        // }

        ExecutorService pools = Executors.newFixedThreadPool(numNodes);
        List<Future<AppendEntriesReply>> list = new ArrayList<Future<AppendEntriesReply>>();
        int[] currentEntryCount = new int[numNodes];

        for (Map.Entry<Integer, Integer> entry : nodeidPortMap.entrySet())
        {
          int otherNodeId = entry.getKey();
          int apPrevLogIndex = nextIndex[otherNodeId];
          int apPrevLogTerm = lastTerm[otherNodeId];
          RaftNodeBlockingStub stub = hostConnectionMap.get(entry.getValue());
          AppendEntriesArgs.Builder requestAppendEntries = AppendEntriesArgs.newBuilder();
            requestAppendEntries.setFrom(apFrom);
            requestAppendEntries.setTo(otherNodeId);
            requestAppendEntries.setTerm(apTerm);
            requestAppendEntries.setLeaderId(apLeaderId);
            requestAppendEntries.setPrevLogIndex(apPrevLogIndex);
            requestAppendEntries.setPrevLogTerm(apPrevLogTerm);
            requestAppendEntries.setLeaderCommit(apLeaderCommit);

          // SET ENTRY LOG
          // if(currentLog.size()!=0 && !isHeartbeat)
          // if(!isHeartbeat)
          // {
            
            if(log.size()!=0){
              // System.out.println(otherNodeId +" nextIndex[otherNodeId]: " + nextIndex[otherNodeId]);
              // System.out.println("Log.size(): " + log.size()+" "+System.currentTimeMillis());
              // for (int i = 0; i < log.size(); i ++) {
              //   System.out.println(i + " Log value: " + log.get(i).getValue()+" "+System.currentTimeMillis());
              // }
              for (int i = nextIndex[otherNodeId]; i < log.size(); i++ ) {
                requestAppendEntries.addEntries(log.get(i));
                isHeartbeat = false;
              }
              currentEntryCount[otherNodeId] = log.size() - nextIndex[otherNodeId];
            }
            else
            {
              for(LogEntry logTemp: currentLog)
              {
                requestAppendEntries.addEntries(logTemp);//.build();
                isHeartbeat = false;
              }
              currentEntryCount[otherNodeId] = currentLog.size();
            }
          // }
          
          AppendEntriesArgs requestAppendEntriesFinal = requestAppendEntries.build();
          Future<AppendEntriesReply> futures = pools.submit(new AppendEntriesTask(requestAppendEntriesFinal, stub));
          list.add(futures);            
        }

        for(Future<AppendEntriesReply> f : list){
          try {
            // AppendEntriesReply respondAppendEntries = stub.appendEntries(requestAppendEntries);
            AppendEntriesReply respondAppendEntries = f.get(130, TimeUnit.MILLISECONDS);
            
            if(respondAppendEntries != null && !isHeartbeat)
            {
              if(respondAppendEntries.getSuccess())
              {
                int otherNodeId = respondAppendEntries.getFrom();
                // System.out.println("nextIndex: " + nextIndex[otherNodeId] + " getMatchIndex: " + respondAppendEntries.getMatchIndex());
                
                // if(!isHeartbeat){
                int rightLimit = 0;
                if(respondAppendEntries.getMatchIndex() > lockCounterList.size())
                  rightLimit = lockCounterList.size();
                else
                  rightLimit = respondAppendEntries.getMatchIndex();

                  for(int i = nextIndex[otherNodeId]; i < rightLimit; i++)
                  {
                    // System.out.println("i :"+i);
                    // System.out.println("lockCounterList size :"+lockCounterList.size());
                    // System.out.println("lockList size :"+lockList.size());
                    lockCounterList.set(i, lockCounterList.get(i)+1);
                    // System.out.println(i +" Lock Counter: "+lockCounterList.get(i));
                    int respondCount = lockCounterList.get(i);
                    if(respondCount < 100 &&(respondCount > numNodes / 2))
                    {
                      if(waiting > 0){
                        Object lock = lockList.get(i);
                        try{
                          synchronized (lock) {
                            lock.notifyAll();
                            // System.out.println("Start notify :"+lock.hashCode());
                          }
                        }catch(Throwable e){
                          System.out.println("Exception catched here :"+e);
                        }
                      }
                      lockCounterList.set(i, 100);
                    }
                  }
                  lastTerm[otherNodeId] = currentTerm;
                  nextIndex[otherNodeId]+= currentEntryCount[respondAppendEntries.getFrom()];
                // }
                // System.out.println(respondAppendEntries.getFrom() +" lastTerm: " + lastTerm[respondAppendEntries.getFrom()]+" "+System.currentTimeMillis());
                // System.out.println(respondAppendEntries.getFrom() + " nextIndex: " + nextIndex[respondAppendEntries.getFrom()]);
                // respondCount++;
              }
            }
          } catch (InterruptedException e) {
            System.out.println("InterruptedException");
            e.printStackTrace();
          } catch (ExecutionException e) {
            System.out.println("ExecutionException");
            e.printStackTrace();
          } catch (TimeoutException e) {
            System.out.println(nodeId + " TimeoutException in sending to: " + (list.indexOf(f) + 1));
            e.printStackTrace();
          }
        }
      }
    }

    class RequestVoteTask implements Callable<RequestVoteReply>{
      RequestVoteArgs requestVoteArgs;
      RaftNodeBlockingStub stub;
      public RequestVoteTask(RequestVoteArgs requestVoteArgs, RaftNodeBlockingStub stub) {
        this.requestVoteArgs = requestVoteArgs;
        this.stub = stub;
      }
      public RequestVoteReply call() throws Exception {
        RequestVoteReply respondRequestVote = stub.requestVote(requestVoteArgs);
        return respondRequestVote;
      }
    }

    class AppendEntriesTask implements Callable<AppendEntriesReply>{
      AppendEntriesArgs appendEntriesArgs;
      RaftNodeBlockingStub stub;
      public AppendEntriesTask(AppendEntriesArgs appendEntriesArgs, RaftNodeBlockingStub stub) {
        this.appendEntriesArgs = appendEntriesArgs;
        this.stub = stub;
      }
      public AppendEntriesReply call() throws Exception {
        AppendEntriesReply respondAppendEntries = stub.appendEntries(appendEntriesArgs);
        return respondAppendEntries;
      }
    }

    // Desc:
    // Propose initializes proposing a new operation, and replies with the
    // result of committing this operation. Propose should not return until
    // this operation has been committed, or this node is not leader now.
    //
    // If the we put a new <k, v> pair or deleted an existing <k, v> pair
    // successfully, it should return OK; If it tries to delete an non-existing
    // key, a KeyNotFound should be returned; If this node is not leader now,
    // it should return WrongNode as well as the currentLeader id.
    //
    // Params:
    // args: the operation to propose
    // reply: as specified in Desc
    @Override
    public void propose(ProposeArgs request, StreamObserver<ProposeReply> responseObserver) {
      // TODO: Implement this!
      Operation op = request.getOp();
      String key = request.getKey();
      int v = request.getV();

      Object lock = new Object();
      lockList.add(lock);
      int lockCounter = 1;
      lockCounterList.add(lockCounter);
      lockIndex++;

      ProposeReply respond = ProposeReply.newBuilder()
          .setCurrentLeader(currentLeader)
          .setStatus(Status.OK)
          .build();

      if(currentLeader != nodeId)
      {
        respond = ProposeReply.newBuilder()
              .setCurrentLeader(currentLeader)
              .setStatus(Status.WrongNode)
              .build();
      }
      else
      {
        if(op.equals(Operation.Put))
        {
          LogEntry currentLogTemp = LogEntry.newBuilder()
            .setTerm(currentTerm)
            .setOp(Operation.Put)
            .setKey(key)
            .setValue(v)
            .build();

          currentLog.add(currentLogTemp);
          log.add(currentLogTemp);
          
          synchronized (lock) {
            try{
                testString = "PutBeforeWait";
                waiting++;
                lock.wait();
                testString = "PutAfterWait";
                waiting--;
                currentLog = new ArrayList<LogEntry>();
                // currentLog.add(currentLogTemp);
                // log.add(currentLogTemp);
            }catch(InterruptedException e){
                e.printStackTrace();
            }
          }
          respond = ProposeReply.newBuilder()
              .setCurrentLeader(currentLeader)
              .setStatus(Status.OK)
              .build();
        }
        else if(op.equals(Operation.Delete))
        {
          LogEntry currentLogTemp = LogEntry.newBuilder()
                .setTerm(currentTerm)
                .setOp(Operation.Delete)
                .setKey(key)
                .setValue(0)
                // .setValue(logTemp.getValue())
                .build();
          currentLog.add(currentLogTemp);
          log.add(currentLogTemp);
          synchronized (lock) {
            try{
                testString = "DeleteBeforeWait";
                waiting++;
                lock.wait();
                testString = "DeleteAfterWait";
                waiting--;
                // currentLog = new ArrayList<LogEntry>();
                // currentLog.add(currentLogTemp);
                // log.add(currentLogTemp);
            }catch(InterruptedException e){
                e.printStackTrace();
            }
          }
          respond = ProposeReply.newBuilder()
            .setCurrentLeader(currentLeader)
            .setStatus(Status.KeyNotFound)
            .build();
        }
        // COMMIT
        if(op.equals(Operation.Put))
        {
          if(keyValueMap.get(key)!= null)
          {
            keyValueMap.remove(key);
          }
          keyValueMap.put(key, v);
        }
        else if(op.equals(Operation.Delete))
        {
          if(keyValueMap.get(key)!= null)
          {
            keyValueMap.remove(key);
            respond = ProposeReply.newBuilder()
              .setCurrentLeader(currentLeader)
              .setStatus(Status.OK)
              .build();
          }
        }
        leaderCommitIndex++;
        
      }
      responseObserver.onNext(respond);
      responseObserver.onCompleted();
    }

    // Desc:GetValue
    // GetValue looks up the value for a key, and replies with the value or with
    // the Status KeyNotFound.
    //
    // Params:
    // args: the key to check
    // reply: the value and status for this lookup of the given key
    @Override
    public void getValue(GetValueArgs request, StreamObserver<GetValueReply> responseObserver) {
      // TODO: Implement this!
      String key = request.getKey();
      GetValueReply respond;
      if(keyValueMap.containsKey(key))
      {
        int value = keyValueMap.get(key);
        respond = GetValueReply.newBuilder()
          .setV(value)
          .setStatus(Status.KeyFound)
          .build();
      }
      else
      {
        respond = GetValueReply.newBuilder()
          .setV(0)
          .setStatus(Status.KeyNotFound)
          .build();
      }
      responseObserver.onNext(respond);
      responseObserver.onCompleted();
    }

    // Desc:
    // Receive a RecvRequestVote message from another Raft Node. Check the paper for more details.
    //
    // Params:
    // args: the RequestVote Message, you must include From(src node id) and To(dst node id) when
    // you call this API
    // reply: the RequestVote Reply Message
    @Override
    public void requestVote(RequestVoteArgs request,
        StreamObserver<RequestVoteReply> responseObserver) {
      // TODO: Implement this!
      int from = request.getTo();
      int to = request.getFrom();
      int term = request.getTerm();
      int candidateId = request.getCandidateId();
      int lastLogIndex = request.getLastLogIndex();
      int lastLogTerm = request.getLastLogTerm();

      RequestVoteReply respond;
      if(term > currentTerm && !serverState.equals(ServerState.LEADER))
        grantedVote = false;

      if(term <= currentTerm || serverState.equals(ServerState.LEADER) || grantedVote)
      {
        // if(term > currentTerm){
        //   respond = RequestVoteReply.newBuilder()
        //             .setFrom(from)
        //             .setTo(to)
        //             .setTerm(term)
        //             .setVoteGranted(false)
        //             .build();
        // }
        // else
        respond = RequestVoteReply.newBuilder()
                    .setFrom(from)
                    .setTo(to)
                    .setTerm(currentTerm)
                    .setVoteGranted(false)
                    .build();
      }
      else
      {
        if(term > currentTerm)
        {
          serverState = ServerState.FOLLOWER;
          currentTerm = term;
        }
        respond = RequestVoteReply.newBuilder()
                    .setFrom(from)
                    .setTo(to)
                    .setTerm(term)
                    .setVoteGranted(true)
                    .build();
        grantedVote = true;
      }
      
      responseObserver.onNext(respond);
      responseObserver.onCompleted();
    }

    


    // Desc:
    // Receive a RecvAppendEntries message from another Raft Node. Check the paper for more details.
    //
    // Params:
    // args: the AppendEntries Message, you must include From(src node id) and To(dst node id) when
    // you call this API
    // reply: the AppendEntries Reply Message
    @Override
    public void appendEntries(AppendEntriesArgs request,
        StreamObserver<AppendEntriesReply> responseObserver) {
      // TODO: Implement this!

      int from = request.getTo();
      int to = request.getFrom();
      int term = request.getTerm();
      int leaderId = request.getLeaderId();
      int prevLogIndex = request.getPrevLogIndex();
      int prevLogTerm = request.getPrevLogTerm();
      List<LogEntry> entries = request.getEntriesList();
      int leaderCommit = request.getLeaderCommit();

      getHeartBeatSignal = true;
      serverState = ServerState.FOLLOWER;
      grantedVote = false;
      currentLeader = leaderId;

      AppendEntriesReply respond;

      if(term < currentTerm)
      {
        respond = AppendEntriesReply.newBuilder()
              .setFrom(from)
              .setTo(to)
              .setTerm(currentTerm)
              .setSuccess(false)
              .setMatchIndex(prevLogIndex)
              .build();

          responseObserver.onNext(respond);
          responseObserver.onCompleted();
          return;
      }

      int matchCount = entries.size();

      if(entries.size() != 0)
      {
        for (int i = 0; i < entries.size(); i++) 
        {
            for(LogEntry logTemp: log)
            {
              if(logTemp.getKey().equals(entries.get(i).getKey()) && logTemp.getValue()==entries.get(i).getValue())
              {
                matchCount--;
                break;
              }
            }
            log.add(entries.get(i));
        }
        
      }


      for (int i = 0; i < leaderCommit; i++) {
        String key = log.get(i).getKey();
        int v = log.get(i).getValue();
        Operation op = log.get(i).getOp();
        if(op.equals(Operation.Put))
        {
          if(keyValueMap.get(key)!= null)
          {
            keyValueMap.remove(key);
          }
          keyValueMap.put(key, v);
        }
        else if(op.equals(Operation.Delete))
        {
          if(keyValueMap.get(key)!= null)
          {
            keyValueMap.remove(key);
          }
        }
      }

      // for(LogEntry log1: log)
      // {
      //   for(LogEntry log2: log)
      //   {
      //     if(log1.getKey().equals(log2.getKey()) && log1.getValue()==log2.getValue() && log.indexOf(log1) < log.indexOf(log2))
      //       matchCount++;
      //   }
      // }

      
      matchIndex.add(matchIndex.get(matchIndex.size() - 1) + matchCount);
      // matchIndex.add(matchCount);
      // if(matchIndex.get(matchIndex.size() - 1) > entries.size())
        // matchCount = matchIndex.get(matchIndex.size() - 1);

      // matchIndex.add(matchCount);
      prevLogIndex = matchIndex.get(matchIndex.size() - 1);
      prevLogTerm = currentTerm;

      if(leaderCommit > commitIndex)
      {
        commitIndex = Math.min(leaderCommit, log.size());
      }
      // int matchIndex = nextIndex[nodeId] + entries.size();
      
      respond = AppendEntriesReply.newBuilder()
        .setFrom(from)
        .setTo(to)
        .setTerm(term)
        .setSuccess(true)
        .setMatchIndex(matchIndex.get(matchIndex.size() - 1))
        .build();
      responseObserver.onNext(respond);
      responseObserver.onCompleted();
    }

    // Desc:
    // Set electionTimeOut as args.Timeout milliseconds.
    // You also need to stop current ticker and reset it to fire every args.Timeout milliseconds.
    //
    // Params:
    // args: the heartbeat duration
    // reply: no use
    @Override
    public void setElectionTimeout(SetElectionTimeoutArgs request,
        StreamObserver<SetElectionTimeoutReply> responseObserver) {
      // TODO: Implement this!
      electionTimeChangeFinished = false;
      if(!serverState.equals(ServerState.LEADER))
      {
        if(timer != null)
        {
          timer.cancel();
          electionTimeout = request.getTimeout();
          timer = new Timer();
        }
        else
          electionTimeout = request.getTimeout();
        
      }
      else if(serverState.equals(ServerState.LEADER))
      {
        if(leaderTimer != null)
        {
          leaderTimer.cancel();
          electionTimeout = request.getTimeout();
          leaderTimer = new Timer();
        }
        else
          electionTimeout = request.getTimeout();
      }
      
      mainIteration();
      electionTimeChangeFinished = true;
    }

    // Desc:
    // Set heartBeatInterval as args.Interval milliseconds.
    // You also need to stop current ticker and reset it to fire every args.Interval milliseconds.
    //
    // Params:
    // args: the heartbeat duration
    // reply: no use
    @Override
    public void setHeartBeatInterval(SetHeartBeatIntervalArgs request,
        StreamObserver<SetHeartBeatIntervalReply> responseObserver) {
      // TODO: Implement this!
      try{
        Thread.sleep(10);
      }catch (InterruptedException e) {
            System.out.println("InterruptedException");
            e.printStackTrace();
      }   
      if(electionTimeChangeFinished)
      {
        if(!serverState.equals(ServerState.LEADER))
        {
          if(timer != null)
          {
            // followerTask.cancel();
            timer.cancel();
            heartBeatInterval = request.getInterval();
            timer = new Timer();
          }
          else
            heartBeatInterval = request.getInterval();

        }
        else if(serverState.equals(ServerState.LEADER))
        {
          if(leaderTimer != null)
          {
            // leaderTask.cancel();
            leaderTimer.cancel();
            heartBeatInterval = request.getInterval();
            leaderTimer = new Timer();
          }
          else
            heartBeatInterval = request.getInterval();
        }
        mainIteration();
      }
      else
      {
        heartBeatInterval = request.getInterval();
      }
      
    }

    //NO NEED TO TOUCH THIS FUNCTION
    @Override
    public void checkEvents(CheckEventsArgs request,
        StreamObserver<CheckEventsReply> responseObserver) {
    }

    public Server getGrpcServer(){
      return this.server;
    }
  }
}