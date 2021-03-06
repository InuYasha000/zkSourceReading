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

package org.apache.zookeeper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.zookeeper.ClientCnxn.EndOfStreamException;
import org.apache.zookeeper.ClientCnxn.Packet;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.client.ZKClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientCnxnSocketNIO extends ClientCnxnSocket {
    private static final Logger LOG = LoggerFactory
            .getLogger(ClientCnxnSocketNIO.class);

    private final Selector selector = Selector.open();

    private SelectionKey sockKey;

    //套接字绑定到的本地地址
    private SocketAddress localSocketAddress;

    //返回套接字连接到的远程地址
    private SocketAddress remoteSocketAddress;

    ClientCnxnSocketNIO(ZKClientConfig clientConfig) throws IOException {
        this.clientConfig = clientConfig;
        initProperties();
    }

    @Override
    boolean isConnected() {
        return sockKey != null;
    }
    
    /**
     * @return true if a packet was received
     * @throws InterruptedException
     * @throws IOException
     */
    //1--回调事件和发送请求
    void doIO(List<Packet> pendingQueue, ClientCnxn cnxn)
      throws InterruptedException, IOException {
        SocketChannel sock = (SocketChannel) sockKey.channel();
        if (sock == null) {
            throw new IOException("Socket is null!");
        }
        //1--回调（太菜了，竟然这么认为）
        //有数据可以读取，或者是zk服务端返回给你的响应
        if (sockKey.isReadable()) {
            int rc = sock.read(incomingBuffer);
            if (rc < 0) {
                throw new EndOfStreamException(
                        "Unable to read additional data from server sessionid 0x"
                                + Long.toHexString(sessionId)
                                + ", likely server has closed socket");
            }
            //第一次读取了4个字节的头，这里是判断有没有拆包
            if (!incomingBuffer.hasRemaining()) {
                incomingBuffer.flip();
                //这里是判断当前读是读的长度还是读的内容，因为这个是循环，每次读的内容不一样
                if (incomingBuffer == lenBuffer) {
                    recvCount.getAndIncrement();
                    readLength();
                } else if (!initialized) {//1--也就是当前客户端和服务端之间正在进行会话创建，比如createSession
                    //1--首先判断当前的客户端状态是否是“已初始化”
                    //将接收到的ByteBuffer(incomebuffer)序列化为ConnectRequest对象
                    readConnectResult();
                    enableRead();
                    if (findSendablePacket(outgoingQueue,
                            sendThread.tunnelAuthInProgress()) != null) {
                        // Since SASL authentication has completed (if client is configured to do so),
                        // outgoing packets waiting in the outgoingQueue can now be sent.
                        enableWrite();
                    }
                    lenBuffer.clear();
                    incomingBuffer = lenBuffer;
                    updateLastHeard();
                    initialized = true;
                } else {//1--常规请求，create，getdata，exist，事件响应也在里面
                    sendThread.readResponse(incomingBuffer);
                    lenBuffer.clear();
                    incomingBuffer = lenBuffer;
                    updateLastHeard();
                }
            }
        }
        //当前网络通道可以写
        if (sockKey.isWritable()) {
            //---这里才是真正发送给服务端
            //4--并把Packet放入pendingQueue(CLientCnxn接受服务端返回结果的队列)，以便等待服务端响应后进行相应的处理
            //4--p.requestHeader != null&& p.requestHeader.getType() != OpCode.ping&& p.requestHeader.getType() != OpCode.auth
            //不是很明白这里为什么要传参进去，而不是在方法中直接对outgoingQueue操作
            Packet p = findSendablePacket(outgoingQueue,
                    sendThread.tunnelAuthInProgress());

            if (p != null) {
                updateLastSend();
                // If we already started writing p, p.bb will already exist
                if (p.bb == null) {
                    if ((p.requestHeader != null) &&
                            (p.requestHeader.getType() != OpCode.ping) &&
                            (p.requestHeader.getType() != OpCode.auth)) {
                        //生成一个客户端请求序号xid设置到packet请求头里面去
                        p.requestHeader.setXid(cnxn.getXid());
                    }
                    p.createBB();
                }
                //把这个packet数据采用ByteBuffer的模式通过socket写出去
                sock.write(p.bb);
                //处理拆包问题
                if (!p.bb.hasRemaining()) {
                    sentCount.getAndIncrement();
                    //这里才会删除
                    outgoingQueue.removeFirstOccurrence(p);
                    if (p.requestHeader != null
                            && p.requestHeader.getType() != OpCode.ping
                            && p.requestHeader.getType() != OpCode.auth) {
                        //1--这个if判断在后面pendingQueue取出来的前面是有判断的（常规的请求，create，getdata，exist）
                        //1--也就是在sendThread.readResponse
                        synchronized (pendingQueue) {
                            //在这里加进去pendingQueue,因为pendingQueue储存发送到服务端但需要等待服务端响应的Packet集合
                            pendingQueue.add(p);
                        }
                    }
                }
            }
            if (outgoingQueue.isEmpty()) {
                // No more packets to send: turn off write interest flag.
                // Will be turned on later by a later call to enableWrite(),
                // from within ZooKeeperSaslClient (if client is configured
                // to attempt SASL authentication), or in either doIO() or
                // in doTransport() if not.

                //发现outgogoingQueue为空，取消OP_WRITE关注，避免发现可以执行OP_WRITE事件但是代码在这里发现队列为空
                disableWrite();
            } else if (!initialized && p != null && !p.bb.hasRemaining()) {
                // On initial connection, write the complete connect request
                // packet, but then disable further writes until after
                // receiving a successful connection response.  If the
                // session is expired, then the server sends the expiration
                // response and immediately closes its end of the socket.  If
                // the client is simultaneously writing on its end, then the
                // TCP stack may choose to abort with RST, in which case the
                // client would never receive the session expired event.  See
                // http://docs.oracle.com/javase/6/docs/technotes/guides/net/articles/connection_release.html
                disableWrite();
            } else {
                // Just in case
                //关注OP_WRITE事件
                enableWrite();
            }
        }
    }

    /**
     * 这个方法并未出队列
     */
    private Packet findSendablePacket(LinkedBlockingDeque<Packet> outgoingQueue,
                                      boolean tunneledAuthInProgres) {
        if (outgoingQueue.isEmpty()) {
            return null;
        }
        // If we've already starting sending the first packet, we better finish
        if (outgoingQueue.getFirst().bb != null || !tunneledAuthInProgres) {
            return outgoingQueue.getFirst();
        }
        // Since client's authentication with server is in progress,
        // send only the null-header packet queued by primeConnection().
        // This packet must be sent so that the SASL authentication process
        // can proceed, but all other packets should wait until
        // SASL authentication completes.
        //1--无header的packet也要被处理
        Iterator<Packet> iter = outgoingQueue.iterator();
        while (iter.hasNext()) {
            Packet p = iter.next();
            if (p.requestHeader == null) {
                // We've found the priming-packet. Move it to the beginning of the queue.
                iter.remove();
                outgoingQueue.addFirst(p);//这里不是造成死循环吗?，这里其实是把 p.requestHeader == null 的放到队列头部
                return p;
            } else {
                // Non-priming packet: defer it until later, leaving it in the queue
                // until authentication completes.
                LOG.debug("deferring non-priming packet {} until SASL authentation completes.", p);
            }
        }
        return null;
    }

    @Override
    void cleanup() {
        if (sockKey != null) {
            SocketChannel sock = (SocketChannel) sockKey.channel();
            sockKey.cancel();
            try {
                sock.socket().shutdownInput();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during shutdown input", e);
                }
            }
            try {
                sock.socket().shutdownOutput();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during shutdown output",
                            e);
                }
            }
            try {
                sock.socket().close();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during socket close", e);
                }
            }
            try {
                sock.close();
            } catch (IOException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring exception during channel close", e);
                }
            }
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SendThread interrupted during sleep, ignoring");
            }
        }
        sockKey = null;
    }
 
    @Override
    void close() {
        try {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Doing client selector close");
            }
            selector.close();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Closed client selector");
            }
        } catch (IOException e) {
            LOG.warn("Ignoring exception during selector close", e);
        }
    }
    
    /**
     * create a socket channel.
     * @return the created socket channel
     * @throws IOException
     */
    SocketChannel createSock() throws IOException {
        SocketChannel sock;
        sock = SocketChannel.open();
        sock.configureBlocking(false);
        sock.socket().setSoLinger(false, -1);
        sock.socket().setTcpNoDelay(true);
        return sock;
    }

    /**
     * register with the selection and connect
     * @param sock the {@link SocketChannel} 
     * @param addr the address of remote host
     * @throws IOException
     */
    void registerAndConnect(SocketChannel sock, InetSocketAddress addr) 
    throws IOException {
        sockKey = sock.register(selector, SelectionKey.OP_CONNECT);
        //这只能算底层物理连接
        boolean immediateConnect = sock.connect(addr);
        if (immediateConnect) {//马上就建立成功了
            sendThread.primeConnection();//申请建立session关联(在建立tcp连接后)
        }
    }

    @Override
    void connect(InetSocketAddress addr) throws IOException {
        SocketChannel sock = createSock();
        try {
           registerAndConnect(sock, addr);
      } catch (IOException e) {
            LOG.error("Unable to open socket to " + addr);
            sock.close();
            throw e;
        }
        initialized = false;

        /*
         * Reset incomingBuffer
         */
        lenBuffer.clear();
        incomingBuffer = lenBuffer;
    }

    /**
     * Returns the address to which the socket is connected.
     * 返回套接字连接到的远程地址
     * 
     * @return ip address of the remote side of the connection or null if not
     *         connected
     */
    @Override
    SocketAddress getRemoteSocketAddress() {
        return remoteSocketAddress;
    }

    /**
     * Returns the local address to which the socket is bound.
     * 返回套接字绑定到的本地地址
     * 
     * @return ip address of the remote side of the connection or null if not
     *         connected
     */
    @Override
    SocketAddress getLocalSocketAddress() {
        return localSocketAddress;
    }
    
    private void updateSocketAddresses() {
        Socket socket = ((SocketChannel) sockKey.channel()).socket();
        localSocketAddress = socket.getLocalSocketAddress();
        remoteSocketAddress = socket.getRemoteSocketAddress();
    }

    @Override
    void packetAdded() {
        wakeupCnxn();
    }

    @Override
    void onClosing() {
        wakeupCnxn();
    }

    private synchronized void wakeupCnxn() {
        selector.wakeup();
    }

    //负责请求的发送和响应
    @Override
    void doTransport(int waitTimeOut, List<Packet> pendingQueue, ClientCnxn cnxn)
            throws IOException, InterruptedException {
        //关注OP_READ和OP_WRITE事件
        selector.select(waitTimeOut);
        Set<SelectionKey> selected;
        synchronized (this) {
            selected = selector.selectedKeys();
        }
        // Everything below and until we get back to the select is
        // non blocking, so time is effectively a constant. That is
        // Why we just have to do this once, here
        updateNow();
        for (SelectionKey k : selected) {
            SocketChannel sc = ((SocketChannel) k.channel());
            if ((k.readyOps() & SelectionKey.OP_CONNECT) != 0) {//这里是再次判断是否已经建立了连接，前面已经判断过了
                if (sc.finishConnect()) {//已经和服务器建立连接成功后
                    updateLastSendAndHeard();
                    updateSocketAddresses();
                    sendThread.primeConnection();
                }
            } else if ((k.readyOps() & (SelectionKey.OP_READ | SelectionKey.OP_WRITE)) != 0) {
                doIO(pendingQueue, cnxn);
            }
        }
        if (sendThread.getZkState().isConnected()) {
            //---在这里取得outgoingQueue，很重要，这个也不是的，错误的解答
            if (findSendablePacket(outgoingQueue,
                    sendThread.tunnelAuthInProgress()) != null) {
                enableWrite();
            }
        }
        selected.clear();
    }

    //TODO should this be synchronized?
    @Override
    void testableCloseSocket() throws IOException {
        LOG.info("testableCloseSocket() called");
        // sockKey may be concurrently accessed by multiple
        // threads. We use tmp here to avoid a race condition
        SelectionKey tmp = sockKey;
        if (tmp!=null) {
           ((SocketChannel) tmp.channel()).socket().close();
        }
    }

    @Override
    void saslCompleted() {
        enableWrite();
    }

    synchronized void enableWrite() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_WRITE) == 0) {
            sockKey.interestOps(i | SelectionKey.OP_WRITE);
        }
    }

    private synchronized void disableWrite() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_WRITE) != 0) {
            sockKey.interestOps(i & (~SelectionKey.OP_WRITE));
        }
    }

    synchronized private void enableRead() {
        int i = sockKey.interestOps();
        if ((i & SelectionKey.OP_READ) == 0) {
            sockKey.interestOps(i | SelectionKey.OP_READ);
        }
    }

    @Override
    void connectionPrimed() {
        sockKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
    }

    Selector getSelector() {
        return selector;
    }

    @Override
    void sendPacket(Packet p) throws IOException {
        SocketChannel sock = (SocketChannel) sockKey.channel();
        if (sock == null) {
            throw new IOException("Socket is null!");
        }
        p.createBB();
        ByteBuffer pbb = p.bb;
        sock.write(pbb);
    }
}
