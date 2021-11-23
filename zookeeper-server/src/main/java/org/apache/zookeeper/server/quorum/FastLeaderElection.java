/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.QuorumCnxManager.Message;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Implementation of leader election using TCP. It uses an object of the class
 * QuorumCnxManager to manage connections. Otherwise, the algorithm is push-based
 * as with the other UDP implementations.
 *
 * There are a few parameters that can be tuned to change its behavior. First,
 * finalizeWait determines the amount of time to wait until deciding upon a leader.
 * This is part of the leader election algorithm.
 */


public class FastLeaderElection implements Election {
    private static final Logger LOG = LoggerFactory.getLogger(FastLeaderElection.class);

    /**
     * Determine how much time a process has to wait
     * once it believes that it has reached the end of
     * leader election.
     */
    final static int finalizeWait = 200;


    /**
     * Upper bound on the amount of time between two consecutive
     * notification checks. This impacts the amount of time to get
     * the system up again after long partitions. Currently 60 seconds.
     */

    final static int maxNotificationInterval = 60000;

    /**
     * Connection manager. Fast leader election uses TCP for
     * communication between peers, and QuorumCnxManager manages
     * such connections.
     */

    QuorumCnxManager manager;


    /**
     * Notifications are messages that let other peers know that
     * a given peer has changed its vote, either because it has
     * joined leader election or because it learned of another
     * peer with higher zxid or same zxid and higher server id
     */

    static public class Notification {
        /*
         * Format version, introduced in 3.4.6
         */

        public final static int CURRENTVERSION = 0x2;
        int version;

        /*
         * Proposed leader
         */
        long leader;

        /*
         * zxid of the proposed leader
         */
        long zxid;

        /*
         * Epoch
         */
        long electionEpoch;

        /*
         * current state of sender
         */
        QuorumPeer.ServerState state;

        /*
         * Address of sender
         */
        long sid;

        QuorumVerifier qv;
        /*
         * epoch of the proposed leader
         */
        long peerEpoch;
    }

    static byte[] dummyData = new byte[0];

    /**
     * Messages that a peer wants to send to other peers.
     * These messages can be both Notifications and Acks
     * of reception of notification.
     */
    static public class ToSend {
        static enum mType {crequest, challenge, notification, ack}

        ToSend(mType type,
               long leader,
               long zxid,
               long electionEpoch,
               ServerState state,
               long sid,
               long peerEpoch,
               byte[] configData) {

            this.leader = leader;
            this.zxid = zxid;
            this.electionEpoch = electionEpoch;
            this.state = state;
            this.sid = sid;
            this.peerEpoch = peerEpoch;
            this.configData = configData;
        }

        /*
         * Proposed leader in the case of notification
         */
        long leader;

        /*
         * id contains the tag for acks, and zxid for notifications
         */
        long zxid;

        /*
         * Epoch
         */
        long electionEpoch;

        /*
         * Current state;
         */
        QuorumPeer.ServerState state;

        /*
         * Address of recipient
         */
        long sid;

        /*
         * Used to send a QuorumVerifier (configuration info)
         */
        byte[] configData = dummyData;

        /*
         * Leader epoch
         */
        long peerEpoch;
    }

    LinkedBlockingQueue<ToSend> sendqueue;
    LinkedBlockingQueue<Notification> recvqueue;

    /**
     * Multi-threaded implementation of message handler. Messenger
     * implements two sub-classes: WorkReceiver and  WorkSender. The
     * functionality of each is obvious from the name. Each of these
     * spawns a new thread.
     */

    protected class Messenger {

        /**
         * Receives messages from instance of QuorumCnxManager on
         * method run(), and processes such messages.
         */

        class WorkerReceiver extends ZooKeeperThread {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerReceiver(QuorumCnxManager manager) {
                super("WorkerReceiver");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {
                Message response;
                while (!stop) {
                    // Sleeps on receive
                    try {
                        response = manager.pollRecvQueue(3000, TimeUnit.MILLISECONDS); // 从传输层接收队列中取出选票
                        if (response == null) continue; // 若未渠道则继续等待下一次获取选票
                        final int capacity = response.buffer.capacity();
                        // The current protocol and two previous generations all send at least 28 bytes
                        if (capacity < 28) { // 当前协议和前两代协议至少发送28bytes
                            LOG.error("Got a short response from server {}: {}", response.sid, capacity);
                            continue;
                        }
                        // this is the backwardCompatibility mode in place before ZK-107
                        // It is for a version of the protocol in which we didn't send peer epoch
                        // With peer epoch and version the message became 40 bytes
                        boolean backCompatibility28 = (capacity == 28);
                        // this is the backwardCompatibility mode for no version information
                        boolean backCompatibility40 = (capacity == 40);
                        response.buffer.clear();
                        // Instantiate Notification and set its attributes
                        Notification n = new Notification();

                        int rstate = response.buffer.getInt();
                        long rleader = response.buffer.getLong();
                        long rzxid = response.buffer.getLong();
                        long relectionEpoch = response.buffer.getLong();
                        long rpeerepoch;

                        int version = 0x0;
                        QuorumVerifier rqv = null;
                        try {
                            if (!backCompatibility28) {
                                rpeerepoch = response.buffer.getLong();
                                if (!backCompatibility40) {
                                    /*
                                     * Version added in 3.4.6
                                     */
                                    version = response.buffer.getInt();
                                } else {
                                    LOG.info("Backward compatibility mode (36 bits), server id: {}", response.sid);
                                }
                            } else {
                                LOG.info("Backward compatibility mode (28 bits), server id: {}", response.sid);
                                rpeerepoch = ZxidUtils.getEpochFromZxid(rzxid);
                            }
                            // check if we have a version that includes config. If so extract config info from message.
                            if (version > 0x1) {
                                int configLength = response.buffer.getInt();
                                // we want to avoid errors caused by the allocation of a byte array with negative length
                                // (causing NegativeArraySizeException) or huge length (causing e.g. OutOfMemoryError)
                                if (configLength < 0 || configLength > capacity) {
                                    throw new IOException(String.format("Invalid configLength in notification message! sid=%d, capacity=%d, version=%d, configLength=%d", response.sid, capacity, version, configLength));
                                }
                                byte b[] = new byte[configLength];
                                response.buffer.get(b);
                                synchronized (self) {
                                    try {
                                        rqv = self.configFromString(new String(b));
                                        QuorumVerifier curQV = self.getQuorumVerifier();
                                        if (rqv.getVersion() > curQV.getVersion()) {
                                            LOG.info("{} Received version: {} my version: {}", self.getId(), Long.toHexString(rqv.getVersion()), Long.toHexString(self.getQuorumVerifier().getVersion()));
                                            if (self.getPeerState() == ServerState.LOOKING) {
                                                LOG.debug("Invoking processReconfig(), state: {}", self.getServerState());
                                                self.processReconfig(rqv, null, null, false);
                                                if (!rqv.equals(curQV)) {
                                                    LOG.info("restarting leader election");
                                                    self.shuttingDownLE = true;
                                                    self.getElectionAlg().shutdown();
                                                    break;
                                                }
                                            } else {
                                                LOG.debug("Skip processReconfig(), state: {}", self.getServerState());
                                            }
                                        }
                                    } catch (IOException | ConfigException e) {
                                        LOG.error("Something went wrong while processing config received from {}. Continue to process the notification message without handling the configuration.", response.sid);
                                    }
                                }
                            } else {
                                LOG.info("Backward compatibility mode (before reconfig), server id: {}", response.sid);
                            }
                        } catch (BufferUnderflowException | IOException e) {
                            LOG.warn("Skipping the processing of a partial / malformed response message sent by sid={} (message length: {})", response.sid, capacity, e);
                            continue;
                        }

                        /*
                         * If it is from a non-voting server (such as an observer or a non-voting follower), respond right away.
                         */
                        if (!validVoter(response.sid)) { // 若来自无投票权的节点则将当前的选票信息返回应用层发送队列中发送给发起方
                            Vote current = self.getCurrentVote();
                            QuorumVerifier qv = self.getQuorumVerifier();
                            ToSend notmsg = new ToSend(ToSend.mType.notification, current.getId(), current.getZxid(), logicalclock.get(), self.getPeerState(), response.sid, current.getPeerEpoch(), qv.toString().getBytes());
                            sendqueue.offer(notmsg); // 将当前的选票信息放回应用层发送队列中
                        } else {
                            // Receive new message
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Receive new notification message. My id = " + self.getId());
                            }
                            // State of peer that sent this message
                            QuorumPeer.ServerState ackstate = QuorumPeer.ServerState.LOOKING;
                            switch (rstate) {
                                case 0:
                                    ackstate = QuorumPeer.ServerState.LOOKING;
                                    break;
                                case 1:
                                    ackstate = QuorumPeer.ServerState.FOLLOWING;
                                    break;
                                case 2:
                                    ackstate = QuorumPeer.ServerState.LEADING;
                                    break;
                                case 3:
                                    ackstate = QuorumPeer.ServerState.OBSERVING;
                                    break;
                                default:
                                    continue;
                            }

                            n.leader = rleader;
                            n.zxid = rzxid;
                            n.electionEpoch = relectionEpoch;
                            n.state = ackstate;
                            n.sid = response.sid;
                            n.peerEpoch = rpeerepoch;
                            n.version = version;
                            n.qv = rqv;
                            /*
                             * Print notification info
                             */
                            if (LOG.isInfoEnabled()) {
                                printNotification(n);
                            }

                            /*
                             * If this server is looking, then send proposed leader
                             */
                            if (self.getPeerState() == QuorumPeer.ServerState.LOOKING) {
                                recvqueue.offer(n); // 是本机且还处于选举状态，放入应用层接收队列，由RecvWorker线程去处理
                                /*
                                 * Send a notification back if the peer that sent this
                                 * message is also looking and its logical clock is
                                 * lagging behind.
                                 */
                                // 若发送选票方是选举状态，且发送选票方选举周期小于自己，则把自己PK出来的选票回发给发送选票方
                                if ((ackstate == QuorumPeer.ServerState.LOOKING) && (n.electionEpoch < logicalclock.get())) {
                                    Vote v = getVote();
                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    ToSend notmsg = new ToSend(ToSend.mType.notification, v.getId(), v.getZxid(), logicalclock.get(), self.getPeerState(), response.sid, v.getPeerEpoch(), qv.toString().getBytes());
                                    sendqueue.offer(notmsg); // 把自己PK出来的选票回发给发送选票方，其实就是当前节点的投票发送回去
                                }
                            } else { // 若本节点状态不是LOOKING但发送选票的节点状态为LOOKING，则说明Leader已经明确，则只需要将Leader的票据信息返回即可
                                /*
                                 * If this server is not looking, but the one that sent the ack is looking, then send back what it believes to be the leader.
                                 */
                                Vote current = self.getCurrentVote(); // 获取本节点当前的投票
                                if (ackstate == QuorumPeer.ServerState.LOOKING) {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("Sending new notification. My id ={} recipient={} zxid=0x{} leader={} config version = {}", self.getId(), response.sid, Long.toHexString(current.getZxid()), current.getId(), Long.toHexString(self.getQuorumVerifier().getVersion()));
                                    }
                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    ToSend notmsg = new ToSend(ToSend.mType.notification, current.getId(), current.getZxid(), current.getElectionEpoch(), self.getPeerState(), response.sid, current.getPeerEpoch(), qv.toString().getBytes());
                                    sendqueue.offer(notmsg); // 把本节点的投票回发给发送选票方
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted Exception while waiting for new message" + e.toString());
                    }
                }
                LOG.info("WorkerReceiver is down");
            }
        }

        /**
         * This worker simply dequeues a message to send and
         * and queues it on the manager's queue.
         */

        class WorkerSender extends ZooKeeperThread {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerSender(QuorumCnxManager manager) {
                super("WorkerSender");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {
                while (!stop) {
                    try {
                        ToSend m = sendqueue.poll(3000, TimeUnit.MILLISECONDS);  // 从应用层发送阻塞队列里获取选票
                        if (m == null) continue;
                        process(m); // 处理取出的选票
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                LOG.info("WorkerSender is down");
            }

            /**
             * Called by run() once there is a new message to send.
             *
             * @param m     message to send
             */
            void process(ToSend m) {
                ByteBuffer requestBuffer = buildMsg(m.state.ordinal(), m.leader, m.zxid, m.electionEpoch, m.peerEpoch, m.configData);
                manager.toSend(m.sid, requestBuffer);
            }
        }

        WorkerSender ws;
        WorkerReceiver wr;
        Thread wsThread = null;
        Thread wrThread = null;

        /**
         * Constructor of class Messenger.
         *
         * @param manager   Connection manager
         */
        Messenger(QuorumCnxManager manager) {
            this.ws = new WorkerSender(manager); // 用于发送选票的异步线程
            this.wsThread = new Thread(this.ws, "WorkerSender[myid=" + self.getId() + "]");
            this.wsThread.setDaemon(true);

            this.wr = new WorkerReceiver(manager); // 用于接收选票的异步线程
            this.wrThread = new Thread(this.wr, "WorkerReceiver[myid=" + self.getId() + "]");
            this.wrThread.setDaemon(true);
        }

        /**
         * Starts instances of WorkerSender and WorkerReceiver
         */
        void start() {
            this.wsThread.start(); // 运行发送选票线程
            this.wrThread.start(); // 运行接收选票线程
        }

        /**
         * Stops instances of WorkerSender and WorkerReceiver
         */
        void halt() {
            this.ws.stop = true;
            this.wr.stop = true;
        }

    }

    QuorumPeer self;
    Messenger messenger;
    AtomicLong logicalclock = new AtomicLong(); /* Election instance */
    long proposedLeader;
    long proposedZxid;
    long proposedEpoch;


    /**
     * Returns the current vlue of the logical clock counter
     */
    public long getLogicalClock() {
        return logicalclock.get();
    }

    static ByteBuffer buildMsg(int state,
                               long leader,
                               long zxid,
                               long electionEpoch,
                               long epoch) {
        byte requestBytes[] = new byte[40];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send, this is called directly only in tests
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(0x1);

        return requestBuffer;
    }

    static ByteBuffer buildMsg(int state,
                               long leader,
                               long zxid,
                               long electionEpoch,
                               long epoch,
                               byte[] configData) {
        byte requestBytes[] = new byte[44 + configData.length];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(Notification.CURRENTVERSION);
        requestBuffer.putInt(configData.length);
        requestBuffer.put(configData);

        return requestBuffer;
    }

    /**
     * Constructor of FastLeaderElection. It takes two parameters, one
     * is the QuorumPeer object that instantiated this object, and the other
     * is the connection manager. Such an object should be created only once
     * by each peer during an instance of the ZooKeeper service.
     *
     * @param self  QuorumPeer that created this object
     * @param manager   Connection manager
     */
    public FastLeaderElection(QuorumPeer self, QuorumCnxManager manager) {
        this.stop = false;
        this.manager = manager;
        starter(self, manager);
    }

    /**
     * This method is invoked by the constructor. Because it is a
     * part of the starting procedure of the object that must be on
     * any constructor of this class, it is probably best to keep as
     * a separate method. As we have a single constructor currently,
     * it is not strictly necessary to have it separate.
     *
     * @param self      QuorumPeer that created this object
     * @param manager   Connection manager
     */
    private void starter(QuorumPeer self, QuorumCnxManager manager) {
        this.self = self;
        proposedLeader = -1;
        proposedZxid = -1;

        sendqueue = new LinkedBlockingQueue<ToSend>();
        recvqueue = new LinkedBlockingQueue<Notification>();
        this.messenger = new Messenger(manager);
    }

    /**
     * This method starts the sender and receiver threads.
     */
    public void start() { // 运行发送选票线程，运行接收选票线程
        this.messenger.start();
    }

    private void leaveInstance(Vote v) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("About to leave FLE instance: leader={}, zxid=0x{}, my id={}, my state={}", v.getId(), Long.toHexString(v.getZxid()), self.getId(), self.getPeerState());
        }
        recvqueue.clear();
    }

    public QuorumCnxManager getCnxManager() {
        return manager;
    }

    volatile boolean stop;

    public void shutdown() {
        stop = true;
        proposedLeader = -1;
        proposedZxid = -1;
        LOG.debug("Shutting down connection manager");
        manager.halt();
        LOG.debug("Shutting down messenger");
        messenger.halt();
        LOG.debug("FLE is down");
    }

    /**
     * Send notifications to all peers upon a change in our vote
     */
    private void sendNotifications() {
        for (long sid : self.getCurrentAndNextConfigVoters()) {
            QuorumVerifier qv = self.getQuorumVerifier();
            // 选票中最大zxid是从内存树中取得
            ToSend notmsg = new ToSend(ToSend.mType.notification, proposedLeader, proposedZxid, logicalclock.get(),
                    QuorumPeer.ServerState.LOOKING, sid, proposedEpoch, qv.toString().getBytes());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sending Notification: " + proposedLeader + " (n.leader), 0x" + Long.toHexString(proposedZxid) + " (n.zxid), 0x" + Long.toHexString(logicalclock.get()) + " (n.round), " + sid + " (recipient), " + self.getId() + " (myid), 0x" + Long.toHexString(proposedEpoch) + " (n.peerEpoch)");
            }
            sendqueue.offer(notmsg); // 给所有其他参与投票的节点发送选票到应用层发送队列
        }
    }

    private void printNotification(Notification n) {
        LOG.info("Notification: "
                + Long.toHexString(n.version) + " (message format version), "
                + n.leader + " (n.leader), 0x"
                + Long.toHexString(n.zxid) + " (n.zxid), 0x"
                + Long.toHexString(n.electionEpoch) + " (n.round), " + n.state
                + " (n.state), " + n.sid + " (n.sid), 0x"
                + Long.toHexString(n.peerEpoch) + " (n.peerEPoch), "
                + self.getPeerState() + " (my state)"
                + (n.qv != null ? (Long.toHexString(n.qv.getVersion()) + " (n.config version)") : ""));
    }


    /**
     * Check if a pair (server id, zxid) succeeds our
     * current vote.
     *
     * @param id    Server identifier
     * @param zxid  Last zxid observed by the issuer of this vote
     */
    protected boolean totalOrderPredicate(long newId, long newZxid, long newEpoch, long curId, long curZxid, long curEpoch) {
        LOG.debug("id: " + newId + ", proposed id: " + curId + ", zxid: 0x" + Long.toHexString(newZxid) + ", proposed zxid: 0x" + Long.toHexString(curZxid));
        if (self.getQuorumVerifier().getWeight(newId) == 0) {
            return false;
        }
        /*
         * We return true if one of the following three cases hold:
         * 1- New epoch is higher   首先比较收到的选票的选举周期，若收到的选票的选举周期大于当前的则返回true
         * 2- New epoch is the same as current epoch, but new zxid is higher 若选举周期相同，比较zxid
         * 3- New epoch is the same as current epoch, new zxid is the same 若选举周期相同，zxid也相同，则比较myid
         *  as current zxid, but server id is higher.
         */
        return ((newEpoch > curEpoch) || ((newEpoch == curEpoch) && ((newZxid > curZxid) || ((newZxid == curZxid) && (newId > curId)))));
    }

    /**
     * Termination predicate. Given a set of votes, determines if have
     * sufficient to declare the end of the election round.
     *
     * @param votes
     *            Set of votes
     * @param vote
     *            Identifier of the vote received last
     */
    protected boolean termPredicate(Map<Long, Vote> votes, Vote vote) { // 过半数选举leader逻辑，判断vote的选票是否超过半数
        SyncedLearnerTracker voteSet = new SyncedLearnerTracker();
        voteSet.addQuorumVerifier(self.getQuorumVerifier());
        if (self.getLastSeenQuorumVerifier() != null && self.getLastSeenQuorumVerifier().getVersion() > self.getQuorumVerifier().getVersion()) {
            voteSet.addQuorumVerifier(self.getLastSeenQuorumVerifier());
        }
        /*
         * First make the views consistent. Sometimes peers will have different zxids for a server depending on timing.
         * 遍历选票箱中收到的选票与本机投的leader选票对比，若相等，则将投票机器sid加入到选票箱
         */
        for (Map.Entry<Long, Vote> entry : votes.entrySet()) {
            if (vote.equals(entry.getValue())) {
                voteSet.addAck(entry.getKey());
            }
        }
        return voteSet.hasAllQuorums(); // 判断投票机器是否超过半数，超过半数返回true
    }

    /**
     * In the case there is a leader elected, and a quorum supporting
     * this leader, we have to check if the leader has voted and acked
     * that it is leading. We need this check to avoid that peers keep
     * electing over and over a peer that has crashed and it is no
     * longer leading.
     *
     * @param votes set of votes
     * @param   leader  leader id
     * @param   electionEpoch   epoch id
     */
    protected boolean checkLeader(Map<Long, Vote> votes, long leader, long electionEpoch) {
        boolean predicate = true;
        /*
         * If everyone else thinks I'm the leader, I must be the leader.
         * The other two checks are just for the case in which I'm not the
         * leader. If I'm not the leader and I haven't received a message
         * from leader stating that it is leading, then predicate is false.
         */
        if (leader != self.getId()) {
            if (votes.get(leader) == null) predicate = false;
            else if (votes.get(leader).getState() != ServerState.LEADING) predicate = false;
        } else if (logicalclock.get() != electionEpoch) {
            predicate = false;
        }
        return predicate;
    }

    synchronized void updateProposal(long leader, long zxid, long epoch) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Updating proposal: " + leader + " (newleader), 0x" + Long.toHexString(zxid) + " (newzxid), " + proposedLeader + " (oldleader), 0x" + Long.toHexString(proposedZxid) + " (oldzxid)");
        }
        proposedLeader = leader;
        proposedZxid = zxid;
        proposedEpoch = epoch;
    }

    synchronized public Vote getVote() {
        return new Vote(proposedLeader, proposedZxid, proposedEpoch);
    }

    /**
     * A learning state can be either FOLLOWING or OBSERVING.
     * This method simply decides which one depending on the
     * role of the server.
     *
     * @return ServerState
     */
    private ServerState learningState() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            LOG.debug("I'm a participant: " + self.getId());
            return ServerState.FOLLOWING;
        } else {
            LOG.debug("I'm an observer: " + self.getId());
            return ServerState.OBSERVING;
        }
    }

    /**
     * Returns the initial vote value of server identifier.
     *
     * @return long
     */
    private long getInitId() {
        if (self.getQuorumVerifier().getVotingMembers().containsKey(self.getId()))
            return self.getId();
        else return Long.MIN_VALUE;
    }

    /**
     * Returns initial last logged zxid.
     *
     * @return long
     */
    private long getInitLastLoggedZxid() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT)
            return self.getLastLoggedZxid();
        else return Long.MIN_VALUE;
    }

    /**
     * Returns the initial vote value of the peer epoch.
     *
     * @return long
     */
    private long getPeerEpoch() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT)
            try {
                return self.getCurrentEpoch();
            } catch (IOException e) {
                RuntimeException re = new RuntimeException(e.getMessage());
                re.setStackTrace(e.getStackTrace());
                throw re;
            }
        else return Long.MIN_VALUE;
    }

    /**
     * Starts a new round of leader election. Whenever our QuorumPeer
     * changes its state to LOOKING, this method is invoked, and it
     * sends notifications to all other peers.
     */
    public Vote lookForLeader() throws InterruptedException {
        try {
            self.jmxLeaderElectionBean = new LeaderElectionBean();
            MBeanRegistry.getInstance().register(self.jmxLeaderElectionBean, self.jmxLocalPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            self.jmxLeaderElectionBean = null;
        }
        if (self.start_fle == 0) {
            self.start_fle = Time.currentElapsedTime();
        }
        try {
            HashMap<Long, Vote> recvset = new HashMap<Long, Vote>();
            HashMap<Long, Vote> outofelection = new HashMap<Long, Vote>();
            int notTimeout = finalizeWait;
            synchronized (this) {
                logicalclock.incrementAndGet(); // 选举周期加一
                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch()); // 初始化选票为自己
            }
            LOG.info("New election. My id =  " + self.getId() + ", proposed zxid=0x" + Long.toHexString(proposedZxid));
            sendNotifications(); // 发送选票选自己
            /*
             * Loop in which we exchange notifications until we find a leader
             * 当前节点是选举状态会不断从应用层接收队列中获取选票做选举
             */
            while ((self.getPeerState() == ServerState.LOOKING) && (!stop)) {
                /*
                 * Remove next notification from queue, times out after 2 times the termination time
                 * 从队列中删除下一个通知，在终止时间的 2 倍后超时
                 */
                Notification n = recvqueue.poll(notTimeout, TimeUnit.MILLISECONDS);

                /*
                 * Sends more notifications if haven't received enough. Otherwise processes new notification.
                 */
                if (n == null) { // 第一次启动时肯定没有选票，这时会跟需要发送选票的机器建立连接
                    if (manager.haveDelivered()) {
                        sendNotifications();
                    } else {
                        manager.connectAll();
                    }
                    /*
                     * Exponential backoff
                     */
                    int tmpTimeOut = notTimeout * 2;
                    notTimeout = (tmpTimeOut < maxNotificationInterval ? tmpTimeOut : maxNotificationInterval);
                    LOG.info("Notification time out: " + notTimeout);
                } else if (validVoter(n.sid) && validVoter(n.leader)) { // 判断发送选票方的选票状态
                    /*
                     * Only proceed if the vote comes from a replica in the current or next voting view for a replica in the current or next voting view.
                     */
                    switch (n.state) { // 根据发送方的状态走不同的逻辑
                        case LOOKING: // 选举主线
                            // If notification > current, replace and send messages out
                            // 接收的选票选举周期大于自己的选举周期，1.可能是自己后启动加入集群选举；2.网络中断恢复后加入集群选举；其他机器都选举好几轮了，所以需要更新自己的选举周期到最新
                            if (n.electionEpoch > logicalclock.get()) {
                                logicalclock.set(n.electionEpoch); // 更新自己的选举周期到最新
                                recvset.clear(); // 清空之前的选票箱
                                // 选票PK，因为自己选举周期落后，可能是刚加入集群选举，所以是拿收到的选票跟给自己的选票做PK
                                if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, getInitId(), getInitLastLoggedZxid(), getPeerEpoch())) {
                                    updateProposal(n.leader, n.zxid, n.peerEpoch); // 接收到的选票获胜，更新选票
                                } else {
                                    updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch()); // 本节点获胜，更新选票
                                }
                                sendNotifications(); // 发送选票给其他参与选举的所有节点
                            } else if (n.electionEpoch < logicalclock.get()) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("Notification election epoch is smaller than logicalclock. n.electionEpoch = 0x" + Long.toHexString(n.electionEpoch) + ", logicalclock=0x" + Long.toHexString(logicalclock.get()));
                                }
                                break;// 接收的选票选举周期小于自己的选举周期，一般发送选票的机器刚加入到集群选举，发起投它自己的选票，这种选票一般废弃掉
                            } else if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, proposedLeader, proposedZxid, proposedEpoch)) {
                                // 接收的选票的选举周期等于自己，说明大家一直都在参与选举，则在选举PK时需要拿收到的选票跟之前本机投的选票做PK
                                updateProposal(n.leader, n.zxid, n.peerEpoch); // 接收到的选票获胜，更新选票
                                sendNotifications(); // 发送选票给其他参与选举的所有节点
                            }

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Adding vote: from=" + n.sid + ", proposed leader=" + n.leader + ", proposed zxid=0x" + Long.toHexString(n.zxid) + ", proposed election epoch=0x" + Long.toHexString(n.electionEpoch));
                            }
                            // don't care about the version if it's in LOOKING state
                            recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch)); // 将接收到的选票放入选票箱
                            if (termPredicate(recvset, new Vote(proposedLeader, proposedZxid, logicalclock.get(), proposedEpoch))) { // 过半数选举leader逻辑
                                // Verify if there is any change in the proposed leader
                                // 虽然termPredicate已经选出leader，但需要查看是否有新选票加入，若有再做一次PK，若新选票获胜则重新选举
                                while ((n = recvqueue.poll(finalizeWait, TimeUnit.MILLISECONDS)) != null) {
                                    if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, proposedLeader, proposedZxid, proposedEpoch)) {
                                        recvqueue.put(n);
                                        break; // 若新选票获胜则重新选举，则退出重新选举
                                    }
                                }
                                /*
                                 * This predicate is true once we don't read any new relevant message from the reception queue
                                 */
                                if (n == null) {
                                    // 若选出的Leader节点是当前节点，则设置为LEADING，否则设置为FOLLOWING或OBSERVING
                                    self.setPeerState((proposedLeader == self.getId()) ? ServerState.LEADING : learningState());
                                    Vote endVote = new Vote(proposedLeader, proposedZxid, logicalclock.get(), proposedEpoch);
                                    leaveInstance(endVote);
                                    return endVote; // 返回选出的Leader并设置到本节点的currentVote属性中
                                }
                            }
                            break;
                        case OBSERVING: // 若是OBSERVING状态的节点则不参与选举
                            LOG.debug("Notification from observer: " + n.sid);
                            break;
                        case FOLLOWING:
                        case LEADING:
                            /*
                             * Consider all notifications from the same epoch together.
                             * FOLLOWING和LEADING状态的都会走该逻辑，这种状态一般是已经选出leader的集群有新机器加入，新机器处于LOKING状态，会先将选票投给自己
                             * 其他机器接收到后会发已选出的集群leader选票给该机器，该选票的发送方状态就是LEADING或FOLLOWING
                             */
                            if (n.electionEpoch == logicalclock.get()) { // 若选举周期相等
                                recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));
                                // 判断n的选票是否超过半数
                                if (termPredicate(recvset, new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state))
                                        && checkLeader(outofelection, n.leader, n.electionEpoch)) {
                                    self.setPeerState((n.leader == self.getId()) ? ServerState.LEADING : learningState());
                                    Vote endVote = new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
                                    leaveInstance(endVote);
                                    return endVote;
                                }
                            }
                            /*
                             * Before joining an established ensemble, verify that a majority are following the same leader.
                             */
                            outofelection.put(n.sid, new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));
                            if (termPredicate(outofelection, new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state))
                                    && checkLeader(outofelection, n.leader, n.electionEpoch)) {
                                synchronized (this) {
                                    logicalclock.set(n.electionEpoch);
                                    self.setPeerState((n.leader == self.getId()) ? ServerState.LEADING : learningState());
                                }
                                Vote endVote = new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
                                leaveInstance(endVote);
                                return endVote;
                            }
                            break;
                        default:
                            LOG.warn("Notification state unrecoginized: " + n.state + " (n.state), " + n.sid + " (n.sid)");
                            break;
                    }
                } else {
                    if (!validVoter(n.leader)) {
                        LOG.warn("Ignoring notification for non-cluster member sid {} from sid {}", n.leader, n.sid);
                    }
                    if (!validVoter(n.sid)) {
                        LOG.warn("Ignoring notification for sid {} from non-quorum member sid {}", n.leader, n.sid);
                    }
                }
            }
            return null;
        } finally {
            try {
                if (self.jmxLeaderElectionBean != null) {
                    MBeanRegistry.getInstance().unregister(self.jmxLeaderElectionBean);
                }
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
            LOG.debug("Number of connection processing threads: {}", manager.getConnectionThreadCount());
        }
    }

    /**
     * Check if a given sid is represented in either the current or
     * the next voting view
     *
     * @param sid     Server identifier
     * @return boolean
     */
    private boolean validVoter(long sid) {
        return self.getCurrentAndNextConfigVoters().contains(sid);
    }
}
