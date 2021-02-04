/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.apache.jute.Record;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.TxnHeader;
/**
 * This class has the control logic for the Follower.
 * 处理客户端非事务请求，转发事务请求给Leader服务器
 * 参与事务请求Proposal的投票
 * 参与Leader选举投票
 */
public class Follower extends Learner{

    private long lastQueued;
    // This is the same object as this.zk, but we cache the downcast op
    final FollowerZooKeeperServer fzk;
    
    Follower(QuorumPeer self,FollowerZooKeeperServer zk) {
        this.self = self;
        this.zk=zk;
        this.fzk = zk;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Follower ").append(sock);
        sb.append(" lastQueuedZxid:").append(lastQueued);
        sb.append(" pendingRevalidationCount:")
            .append(pendingRevalidations.size());
        return sb.toString();
    }

    /**
     * the main method called by the follower to follow the leader
     *
     * @throws InterruptedException
     */
    //6--首先和Leader同步数据，然后FollowerZookeeperServer，额外启动while循环等待Peer的QuorumPacket包，调用process()处理这些包
    void followLeader() throws InterruptedException {
        self.end_fle = Time.currentElapsedTime();
        long electionTimeTaken = self.end_fle - self.start_fle;
        self.setElectionTimeTaken(electionTimeTaken);
        LOG.info("FOLLOWING - LEADER ELECTION TOOK - {} {}", electionTimeTaken,
                QuorumPeer.FLE_TIME_UNIT);
        self.start_fle = 0;
        self.end_fle = 0;
        fzk.registerJMX(new FollowerBean(this, zk), self.jmxLocalPeerBean);
        try {
            QuorumServer leaderServer = findLeader();            
            try {
                //6--1：链接到Leader，链接成功后Leader会创建一个LeaderHandler专门处理该Follower之间的QuorumPacket消息的传递
                //最多连接5次
                connectToLeader(leaderServer.addr, leaderServer.hostname);
                //6--2：交换各自sid，zxid，Epoch，以此leader决定事务同步方式，注册到leader
                long newEpochZxid = registerWithLeader(Leader.FOLLOWERINFO);
                if (self.isReconfigStateChange())
                   throw new Exception("learned about role change");
                //check to see if the leader zxid is lower than ours
                //this should never happen but is just a safety check
                long newEpoch = ZxidUtils.getEpochFromZxid(newEpochZxid);
                if (newEpoch < self.getAcceptedEpoch()) {
                    LOG.error("Proposed leader epoch " + ZxidUtils.zxidToString(newEpochZxid)
                            + " is less than our accepted epoch " + ZxidUtils.zxidToString(self.getAcceptedEpoch()));
                    throw new IOException("Error: Epoch of leader is lower");
                }
                //6--3：和leader事务数据同步，处理Snap/Diff/TRUNC包
                syncWithLeader(newEpochZxid);                
                QuorumPacket qp = new QuorumPacket();
                //--等待Peer的QuorumPacket包
                while (this.isRunning()) {
                    //这里是依托于外部QuorumPeer的循环
                    readPacket(qp);
                    processPacket(qp);
                }
            } catch (Exception e) {
                LOG.warn("Exception when following the leader", e);
                try {
                    sock.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
    
                // clear pending revalidations
                pendingRevalidations.clear();
            }
        } finally {
            zk.unregisterJMX((Learner)this);
        }
    }

    /**
     * Examine the packet received in qp and dispatch based on its contents.
     * @param qp
     * @throws IOException
     */
    protected void processPacket(QuorumPacket qp) throws Exception{
        switch (qp.getType()) {
        case Leader.PING:            
            ping(qp);            
            break;
            //6--leader将要执行的写命令，只有收到commit命令才真正执行PROPOSAL命令
            //6--严格按照顺序来执行（没看到具体的顺序）
            //6--需要回复ack消息等待leader，Follower向leader回复ack，Observer不需要
        case Leader.PROPOSAL:
            //6-- PROPOSAL是Leader将要执行的写事务命令，COMMIT是提交命令，有收到commit命令才真正执行PROPOSAL命令
            //6-- 同一个写事务在Leader和多个Follower上都执行一次，保证集群数据的一致性
            TxnHeader hdr = new TxnHeader();
            //反序列化
            Record txn = SerializeUtils.deserializeTxn(qp.getData(), hdr);
            if (hdr.getZxid() != lastQueued + 1) {
                LOG.warn("Got zxid 0x"
                        + Long.toHexString(hdr.getZxid())
                        + " expected 0x"
                        + Long.toHexString(lastQueued + 1));
            }
            lastQueued = hdr.getZxid();
            
            if (hdr.getType() == OpCode.reconfig){
               SetDataTxn setDataTxn = (SetDataTxn) txn;       
               QuorumVerifier qv = self.configFromString(new String(setDataTxn.getData()));
               self.setLastSeenQuorumVerifier(qv, true);                               
            }
            //--持久化Proposal，等待COMMIT命令菜commit
            fzk.logRequest(hdr, txn);
            break;
        case Leader.COMMIT: //6--提交命令，只有收到commit命令才真正执行PROPOSAL命令
            //6--当Leader认为一个Proposal已被大多数Follower持久化并等待执行后会发送commit，通知Follower可以提交执行该Proposal，通过这种机制保证写操作能被大部分机器执行
            fzk.commit(qp.getZxid());
            break;
            
        case Leader.COMMITANDACTIVATE:
           // get the new configuration from the request
           Request request = fzk.pendingTxns.element();
           SetDataTxn setDataTxn = (SetDataTxn) request.getTxn();                                                                                                      
           QuorumVerifier qv = self.configFromString(new String(setDataTxn.getData()));                                
 
           // get new designated leader from (current) leader's message
           ByteBuffer buffer = ByteBuffer.wrap(qp.getData());    
           long suggestedLeaderId = buffer.getLong();
            boolean majorChange = 
                   self.processReconfig(qv, suggestedLeaderId, qp.getZxid(), true);
           // commit (writes the new config to ZK tree (/zookeeper/config)                     
           fzk.commit(qp.getZxid());
            if (majorChange) {
               throw new Exception("changes proposed in reconfig");
           }
           break;
        case Leader.UPTODATE:
            LOG.error("Received an UPTODATE message after Follower started");
            break;
        case Leader.REVALIDATE:
            revalidate(qp);
            break;
        case Leader.SYNC:
            //强一致性
            //强制让leader等
            fzk.sync();
            break;
        default:
            LOG.warn("Unknown packet type: {}", LearnerHandler.packetToString(qp));
            break;
        }
    }

    /**
     * The zxid of the last operation seen
     * @return zxid
     */
    public long getZxid() {
        try {
            synchronized (fzk) {
                return fzk.getZxid();
            }
        } catch (NullPointerException e) {
            LOG.warn("error getting zxid", e);
        }
        return -1;
    }
    
    /**
     * The zxid of the last operation queued
     * @return zxid
     */
    protected long getLastQueued() {
        return lastQueued;
    }

    @Override
    public void shutdown() {    
        LOG.info("shutdown called", new Exception("shutdown Follower"));
        super.shutdown();
    }
}
