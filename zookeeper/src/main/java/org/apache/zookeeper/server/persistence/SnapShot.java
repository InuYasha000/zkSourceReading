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

package org.apache.zookeeper.server.persistence;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.zookeeper.server.DataTree;

/**
 * snapshot interface for the persistence layer.
 * implement this interface for implementing 
 * snapshots.
 */
//10--主要是序列化和反序列化，QuorumPeer可以快速的将自己完整快照传递给远方QuorumPeer，实现集群间数据同步
public interface SnapShot {
    
    /**
     * deserialize a data tree from the last valid snapshot and 
     * return the last zxid that was deserialized
     * @param dt the datatree to be deserialized into
     * @param sessions the sessions to be deserialized into
     * @return the last zxid that was deserialized from the snapshot
     * @throws IOException
     */
    //10--从序列化文件中恢复数据库和session
    long deserialize(DataTree dt, Map<Long, Integer> sessions) 
        throws IOException;
    
    /**
     * persist the datatree and the sessions into a persistence storage
     * @param dt the datatree to be serialized
     * @param sessions 
     * @throws IOException
     */
    //10--将数据库和session保存到文件
    void serialize(DataTree dt, Map<Long, Integer> sessions, 
            File name) 
        throws IOException;
    
    /**
     * find the most recent snapshot file
     * @return the most recent snapshot file
     * @throws IOException
     */
    File findMostRecentSnapshot() throws IOException;
    
    /**
     * free resources from this snapshot immediately
     * @throws IOException
     */
    void close() throws IOException;
} 
