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

package org.apache.zookeeper.server;

import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.KeeperException.SessionMovedException;

/**
 * This is the basic interface that ZooKeeperServer uses to track sessions. The
 * standalone and leader ZooKeeperServer use the same SessionTracker. The
 * FollowerZooKeeperServer uses a SessionTracker which is basically a simple
 * shell to track information to be forwarded to the leader.
 * 这是zookeperserver用来跟踪会话的基本接口
 * 单机和leader ZooKeeperServer使用相同的SessionTracker
 * Follower ZooKeeperServer使用的SessionTracker是一个简单的shell跟踪要转发给leader的信息
 */
public interface SessionTracker {
    public static interface Session {
        long getSessionId();
        int getTimeout();
        boolean isClosing();
    }
    public static interface SessionExpirer {
        void expire(Session session);

        long getServerId();
    }

    long createSession(int sessionTimeout);

    /**
     * Add a global session to those being tracked.
     * 将全局会话添加到正在跟踪的会话中
     * @param id sessionId
     * @param to sessionTimeout
     * @return whether the session was newly added (if false, already existed)
     */
    boolean addGlobalSession(long id, int to);

    /**
     * Add a session to those being tracked. The session is added as a local
     * session if they are enabled, otherwise as global.
     * 将会话添加到正在跟踪的会话中。会话被添加为本地会话（如果已启用），否则为全局会话
     * @param id sessionId
     * @param to sessionTimeout
     * @return whether the session was newly added (if false, already existed)
     */
    boolean addSession(long id, int to);

    /**
     * @param sessionId
     * @param sessionTimeout
     * @return false if session is no longer active
     */
    boolean touchSession(long sessionId, int sessionTimeout);

    /**
     * Mark that the session is in the process of closing.
     * 标记会话正在结束
     * @param sessionId
     */
    void setSessionClosing(long sessionId);

    /**
     *
     */
    void shutdown();

    /**
     * @param sessionId
     */
    void removeSession(long sessionId);

    /**
     * @param sessionId
     * @return whether or not the SessionTracker is aware of this session
     */
    boolean isTrackingSession(long sessionId);

    /**
     * Checks whether the SessionTracker is aware of this session, the session
     * is still active, and the owner matches. If the owner wasn't previously
     * set, this sets the owner of the session.
     *
     * UnknownSessionException should never been thrown to the client. It is
     * only used internally to deal with possible local session from other
     * machine
     *
     * @param sessionId
     * @param owner
     */
    public void checkSession(long sessionId, Object owner)
            throws KeeperException.SessionExpiredException,
            KeeperException.SessionMovedException,
            KeeperException.UnknownSessionException;

    /**
     * Strictly check that a given session is a global session or not
     * @param sessionId
     * @param owner
     * @throws KeeperException.SessionExpiredException
     * @throws KeeperException.SessionMovedException
     */
    public void checkGlobalSession(long sessionId, Object owner)
            throws KeeperException.SessionExpiredException,
            KeeperException.SessionMovedException;

    void setOwner(long id, Object owner) throws SessionExpiredException;

    /**
     * Text dump of session information, suitable for debugging.
     * @param pwriter the output writer
     */
    void dumpSessions(PrintWriter pwriter);

    /**
     * Returns a mapping of time to session IDs that expire at that time.
     */
    Map<Long, Set<Long>> getSessionExpiryMap();
}
