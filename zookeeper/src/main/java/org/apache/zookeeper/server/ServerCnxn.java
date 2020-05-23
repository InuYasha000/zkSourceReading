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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface to a Server connection - represents a connection from a client
 * to the server.
 */
//ServerCnxn为抽象类，其继承Stats和Watcher两个接口，表示客户端到服务端的连接
//ServerCnxn类维护了很多属性，主要是服务器的统计信息和和命令行信息。
public abstract class ServerCnxn implements Stats, Watcher {
    // This is just an arbitrary object to represent requests issued by
    // (aka owned by) this class
    // 代表由本类提出的请求
    final public static Object me = new Object();
    private static final Logger LOG = LoggerFactory.getLogger(ServerCnxn.class);
    // 认证信息
    protected ArrayList<Id> authInfo = new ArrayList<Id>();

    private static final byte[] fourBytes = new byte[4];

    /**
     * If the client is of old version, we don't send r-o mode info to it.
     * The reason is that if we would, old C client doesn't read it, which
     * results in TCP RST packet, i.e. "connection reset by peer".
     */
    // 是否为旧的C客户端
    boolean isOldClient = true;

    // 获取会话超时时间
    abstract int getSessionTimeout();

    // 关闭
    abstract void close();

    // 发送响应
    public void sendResponse(ReplyHeader h, Record r, String tag) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        // Make space for length
        BinaryOutputArchive bos = BinaryOutputArchive.getArchive(baos);
        try {
            baos.write(fourBytes);
            bos.writeRecord(h, "header");
            if (r != null) {
                bos.writeRecord(r, tag);
            }
            baos.close();
        } catch (IOException e) {
            LOG.error("Error serializing response");
        }
        byte b[] = baos.toByteArray();
        serverStats().updateClientResponseSize(b.length - 4);
        ByteBuffer bb = ByteBuffer.wrap(b);
        bb.putInt(b.length - 4).rewind();
        sendBuffer(bb);
    }

    /* notify the client the session is closing and close/cleanup socket */
    // 关闭会话
    abstract void sendCloseSession();
    // 处理，Watcher接口中的方法
    public abstract void process(WatchedEvent event);
    // 获取会话id
    public abstract long getSessionId();
    // 设置会话id
    abstract void setSessionId(long sessionId);

    /** auth info for the cnxn, returns an unmodifyable list */
    // 获取认证信息，返回不可修改的列表
    public List<Id> getAuthInfo() {
        return Collections.unmodifiableList(authInfo);
    }
    // 添加认证信息
    public void addAuthInfo(Id id) {
        if (authInfo.contains(id) == false) {
            authInfo.add(id);
        }
    }
    // 移除认证信息
    public boolean removeAuthInfo(Id id) {
        return authInfo.remove(id);
    }
    // 设置缓冲
    abstract void sendBuffer(ByteBuffer closeConn);
    // 允许接收
    abstract void enableRecv();
    // 不允许接收
    abstract void disableRecv();
    // 设置会话超时时间
    abstract void setSessionTimeout(int sessionTimeout);

    // Zookeeper的Sasl服务器
    protected ZooKeeperSaslServer zooKeeperSaslServer = null;

    // 请求关闭异常类
    protected static class CloseRequestException extends IOException {
        private static final long serialVersionUID = -7854505709816442681L;

        public CloseRequestException(String msg) {
            super(msg);
        }
    }

    // 流结束异常类
    protected static class EndOfStreamException extends IOException {
        private static final long serialVersionUID = -8255690282104294178L;

        public EndOfStreamException(String msg) {
            super(msg);
        }

        public String toString() {
            return "EndOfStreamException: " + getMessage();
        }
    }
    // 接收的packet
    protected void packetReceived() {
        incrPacketsReceived();
        ServerStats serverStats = serverStats();
        if (serverStats != null) {
            serverStats().incrementPacketsReceived();
        }
    }
    // 发送的packet
    protected void packetSent() {
        incrPacketsSent();
        ServerStats serverStats = serverStats();
        if (serverStats != null) {
            serverStats.incrementPacketsSent();
        }
    }
    // 获取服务器的统计数据
    protected abstract ServerStats serverStats();

    // 创建连接的时间
    protected final Date established = new Date();

    // 接受的packet数量
    protected final AtomicLong packetsReceived = new AtomicLong();
    // 发送的packet数量
    protected final AtomicLong packetsSent = new AtomicLong();

    // 最小延迟
    protected long minLatency;
    // 最大延迟
    protected long maxLatency;
    // 最后操作类型
    protected String lastOp;
    // 最后的cxid
    protected long lastCxid;
    // 最后的zxid
    protected long lastZxid;
    // 最后的响应时间
    protected long lastResponseTime;
    // 最后的延迟
    protected long lastLatency;
    // 数量
    protected long count;
    // 总的延迟
    protected long totalLatency;
    // 重置统计数据
    public synchronized void resetStats() {
        packetsReceived.set(0);
        packetsSent.set(0);
        minLatency = Long.MAX_VALUE;
        maxLatency = 0;
        lastOp = "NA";
        lastCxid = -1;
        lastZxid = -1;
        lastResponseTime = 0;
        lastLatency = 0;

        count = 0;
        totalLatency = 0;
    }

    protected long incrPacketsReceived() {
        return packetsReceived.incrementAndGet();
    }
    
    protected void incrOutstandingRequests(RequestHeader h) {
    }

    protected long incrPacketsSent() {
        return packetsSent.incrementAndGet();
    }

    protected synchronized void updateStatsForResponse(long cxid, long zxid,
            String op, long start, long end)
    {
        // don't overwrite with "special" xids - we're interested
        // in the clients last real operation
        if (cxid >= 0) {
            lastCxid = cxid;
        }
        lastZxid = zxid;
        lastOp = op;
        lastResponseTime = end;
        long elapsed = end - start;
        lastLatency = elapsed;
        if (elapsed < minLatency) {
            minLatency = elapsed;
        }
        if (elapsed > maxLatency) {
            maxLatency = elapsed;
        }
        count++;
        totalLatency += elapsed;
    }

    public Date getEstablished() {
        return (Date)established.clone();
    }

    public abstract long getOutstandingRequests();

    public long getPacketsReceived() {
        return packetsReceived.longValue();
    }

    public long getPacketsSent() {
        return packetsSent.longValue();
    }

    public synchronized long getMinLatency() {
        return minLatency == Long.MAX_VALUE ? 0 : minLatency;
    }

    public synchronized long getAvgLatency() {
        return count == 0 ? 0 : totalLatency / count;
    }

    public synchronized long getMaxLatency() {
        return maxLatency;
    }

    public synchronized String getLastOperation() {
        return lastOp;
    }

    public synchronized long getLastCxid() {
        return lastCxid;
    }

    public synchronized long getLastZxid() {
        return lastZxid;
    }

    public synchronized long getLastResponseTime() {
        return lastResponseTime;
    }

    public synchronized long getLastLatency() {
        return lastLatency;
    }

    /**
     * Prints detailed stats information for the connection.
     *
     * @see dumpConnectionInfo(PrintWriter, boolean) for brief stats
     */
    @Override
    public String toString() {
        StringWriter sw = new StringWriter();
        PrintWriter pwriter = new PrintWriter(sw);
        dumpConnectionInfo(pwriter, false);
        pwriter.flush();
        pwriter.close();
        return sw.toString();
    }

    public abstract InetSocketAddress getRemoteSocketAddress();
    public abstract int getInterestOps();
    public abstract boolean isSecure();
    public abstract Certificate[] getClientCertificateChain();
    public abstract void setClientCertificateChain(Certificate[] chain);
    
    /**
     * Print information about the connection.
     * @param brief iff true prints brief details, otw full detail
     * @return information about this connection
     */
    public synchronized void
    dumpConnectionInfo(PrintWriter pwriter, boolean brief) {
        pwriter.print(" ");
        pwriter.print(getRemoteSocketAddress());
        pwriter.print("[");
        int interestOps = getInterestOps();
        pwriter.print(interestOps == 0 ? "0" : Integer.toHexString(interestOps));
        pwriter.print("](queued=");
        pwriter.print(getOutstandingRequests());
        pwriter.print(",recved=");
        pwriter.print(getPacketsReceived());
        pwriter.print(",sent=");
        pwriter.print(getPacketsSent());

        if (!brief) {
            long sessionId = getSessionId();
            if (sessionId != 0) {
                pwriter.print(",sid=0x");
                pwriter.print(Long.toHexString(sessionId));
                pwriter.print(",lop=");
                pwriter.print(getLastOperation());
                pwriter.print(",est=");
                pwriter.print(getEstablished().getTime());
                pwriter.print(",to=");
                pwriter.print(getSessionTimeout());
                long lastCxid = getLastCxid();
                if (lastCxid >= 0) {
                    pwriter.print(",lcxid=0x");
                    pwriter.print(Long.toHexString(lastCxid));
                }
                pwriter.print(",lzxid=0x");
                pwriter.print(Long.toHexString(getLastZxid()));
                pwriter.print(",lresp=");
                pwriter.print(getLastResponseTime());
                pwriter.print(",llat=");
                pwriter.print(getLastLatency());
                pwriter.print(",minlat=");
                pwriter.print(getMinLatency());
                pwriter.print(",avglat=");
                pwriter.print(getAvgLatency());
                pwriter.print(",maxlat=");
                pwriter.print(getMaxLatency());
            }
        }
        pwriter.print(")");
    }

    public synchronized Map<String, Object> getConnectionInfo(boolean brief) {
        Map<String, Object> info = new LinkedHashMap<String, Object>();
        info.put("remote_socket_address", getRemoteSocketAddress());
        info.put("interest_ops", getInterestOps());
        info.put("outstanding_requests", getOutstandingRequests());
        info.put("packets_received", getPacketsReceived());
        info.put("packets_sent", getPacketsSent());
        if (!brief) {
            info.put("session_id", getSessionId());
            info.put("last_operation", getLastOperation());
            info.put("established", getEstablished());
            info.put("session_timeout", getSessionTimeout());
            info.put("last_cxid", getLastCxid());
            info.put("last_zxid", getLastZxid());
            info.put("last_response_time", getLastResponseTime());
            info.put("last_latency", getLastLatency());
            info.put("min_latency", getMinLatency());
            info.put("avg_latency", getAvgLatency());
            info.put("max_latency", getMaxLatency());
        }
        return info;
    }

    /**
     * clean up the socket related to a command and also make sure we flush the
     * data before we do that
     *
     * @param pwriter
     *            the pwriter for a command socket
     */
    public void cleanupWriterSocket(PrintWriter pwriter) {
        try {
            if (pwriter != null) {
                pwriter.flush();
                pwriter.close();
            }
        } catch (Exception e) {
            LOG.info("Error closing PrintWriter ", e);
        } finally {
            try {
                close();
            } catch (Exception e) {
                LOG.error("Error closing a command socket ", e);
            }
        }
    }
}
