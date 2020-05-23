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

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.txn.ErrorTxn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for performing local session upgrade. Only request submitted
 * directly to the leader should go through this processor.
 */
//--调用链，继承了RequestProcessor，并且设置了nextprocessor
//--FollowerRequestProcessor--Follower的第一个processor
//--ObserverRequestProcessor--Observer的第一个Processor
//--ProposalRequestProcessor--命令提议，群发Proposal到Follower
//--CommitProcessor--事务提交
//--SyncRequestProcessor--持久化Processor到磁盘
//--AckRequestProcessor--Leader给自己ack消息
//--SendAckRequestProcessor--Leader回复ack给Leader
//--toBeAppliedProceessor--等待执行
//--PreRequestProcessor--Leader使用，定义在LeaderRequestProcessor和ProposalRequestProcessor之间，
    // 将写操作记录到outstandingChanges，在FinalRequestProcessor中确保按顺序执行request，防止后续读操作在前一个写操作还未完成时就执行了
//--FinalRequestProcessor--最后的processor，调用ZkDatabase方法最终执行读写操作
public class LeaderRequestProcessor implements RequestProcessor {
    private static final Logger LOG = LoggerFactory
            .getLogger(LeaderRequestProcessor.class);

    private final LeaderZooKeeperServer lzks;

    private final RequestProcessor nextProcessor;

    public LeaderRequestProcessor(LeaderZooKeeperServer zks,
            RequestProcessor nextProcessor) {
        this.lzks = zks;
        this.nextProcessor = nextProcessor;
    }

    @Override
    public void processRequest(Request request)
            throws RequestProcessorException {
        // Check if this is a local session and we are trying to create
        // an ephemeral node, in which case we upgrade the session
        Request upgradeRequest = null;
        try {
            upgradeRequest = lzks.checkUpgradeSession(request);
        } catch (KeeperException ke) {
            if (request.getHdr() != null) {
                LOG.debug("Updating header");
                request.getHdr().setType(OpCode.error);
                request.setTxn(new ErrorTxn(ke.code().intValue()));
            }
            request.setException(ke);
            LOG.info("Error creating upgrade request " + ke.getMessage());
        } catch (IOException ie) {
            LOG.error("Unexpected error in upgrade", ie);
        }
        //--是否存在upgradeRequest的处理请求
        if (upgradeRequest != null) {
            nextProcessor.processRequest(upgradeRequest);
        }

        nextProcessor.processRequest(request);
    }

    @Override
    public void shutdown() {
        LOG.info("Shutting down");
        nextProcessor.shutdown();
    }

}
