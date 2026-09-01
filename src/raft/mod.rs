//! Public Raft node API.
use std::{collections::HashMap, time::Duration};

pub mod election;
pub mod log;
pub mod replication;
pub mod snapshot;
pub mod state;
pub mod storage;


#[derive(Debug, PartialEq, Clone)]
pub struct RaftNode {

    //node identifier and cluster access
    id: NodeId,
    peers: Vec<NodeId>, // All nodes in the cluster

    //Persistent state on all servers before election
    log: Vec<LogEntry>,
    current_term: u64,  //monotonically increasing epoch for distinguishing old message and treating staleness
    voted_for: Option<NodeId>,

    //Volatile state on all nodes, updated on stable storage before responding to RPCs
    state: RaftState,
    leader_id: Option<NodeId>, //current leader of the cluster
    commit_index: u64, //index of highest log entry known to be committed
    last_applied: u64, //index of highest log entry applied to state machine

    //Volatile state on leaders only, reinitialized after election
    next_index: HashMap<NodeId, u64>, //next log index each follower needs
    match_index: HashMap<NodeId, u64>, //highest log index known replicated by followers
    //Question: why does leader need to track the commit index and applied index of all follower nodes?
    //Is there a scenario, where these values different, in order words, follower lagging behind by multipl commits
    //What rules and scoring metrics do leader election follow 

    //Components per Raft node
    storage: Storage,
    transport: Transport,
    state_machine: KVStateMachine,


    //timing parameters
    election_timeout: Duration,
    heartbeat_interval: Duration,

    


}   


impl RaftNode {

    pub fn new(id: NodeId, peers: Vec<NodeId>, storage: Storage, transport: Transport, state_machine: KVStateMachine) -> RaftNode {
        RaftNode {
            id,
            peers,
            log: Vec::new(),
            current_term: 0,
            voted_for: None,
            state: RaftState::Follower,
            leader_id: None,
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            storage,
            transport,
            state_machine,
            election_timeout: Duration::from_millis(500),
            heartbeat_interval: Duration::from_millis(200),
        }
    }

}