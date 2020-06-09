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

package org.apache.zookeeper.server.quorum.flexible;

import java.util.Set;
import java.util.Map;

import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;

/**
 * All quorum validators have to implement a method called
 * containsQuorum, which verifies if a HashSet of server 
 * identifiers constitutes a quorum.
 * 选举算法中的选举算法接口
 */

public interface QuorumVerifier {
    long getWeight(long id);//权重
    boolean containsQuorum(Set<Long> set);//大多数选举算法
    long getVersion();
    void setVersion(long ver);
    Map<Long, QuorumServer> getAllMembers();//返回所有参与者
    Map<Long, QuorumServer> getVotingMembers();//返回选举的所有服务器
    Map<Long, QuorumServer> getObservingMembers();//返回观察者
    boolean equals(Object o);
    String toString();
}
