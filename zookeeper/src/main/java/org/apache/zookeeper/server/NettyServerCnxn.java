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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.security.cert.Certificate;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.server.command.CommandExecutor;
import org.apache.zookeeper.server.command.FourLetterCommands;
import org.apache.zookeeper.server.command.NopCommand;
import org.apache.zookeeper.server.command.SetTraceMaskCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//--zk底层客户端和服务端通信的接口，默认采用Netty组件作为Socket接口，客户端提交的命令会通过NettyServerCnxn到达服务端
public class NettyServerCnxn extends ServerCnxn {
    private static final Logger LOG = LoggerFactory.getLogger(NettyServerCnxn.class);
    // 通道
    private final Channel channel;
    private CompositeByteBuf queuedBuffer;
    // 节流与否
    private final AtomicBoolean throttled = new AtomicBoolean(false);
    // Byte缓冲区
    private ByteBuffer bb;
    // 四个字节的缓冲区
    private final ByteBuffer bbLen = ByteBuffer.allocate(4);
    // 会话ID
    private long sessionId;
    // 会话超时时间
    private int sessionTimeout;
    // 计数
    private AtomicLong outstandingCount = new AtomicLong();
    private Certificate[] clientChain;
    private volatile boolean closingChannel;

    /** The ZooKeeperServer for this connection. May be null if the server
      * is not currently serving requests (for example if the server is not
      * an active quorum participant.
      */
    // Zookeeper服务器
    private volatile ZooKeeperServer zkServer;
    // NettyServerCnxn工厂
    private final NettyServerCnxnFactory factory;
    // 初始化与否
    private boolean initialized;

    NettyServerCnxn(Channel channel, ZooKeeperServer zks, NettyServerCnxnFactory factory) {
        this.channel = channel;
        this.closingChannel = false;
        this.zkServer = zks;
        this.factory = factory;
        if (this.factory.login != null) {// 需要登录信息(用户名和密码登录)
            this.zooKeeperSaslServer = new ZooKeeperSaslServer(factory.login);
        }
    }

    @Override
    public void close() {
        closingChannel = true;

        if (LOG.isDebugEnabled()) {
            LOG.debug("close called for sessionid:0x{}",
                    Long.toHexString(sessionId));
        }

        // ZOOKEEPER-2743:
        // Always unregister connection upon close to prevent
        // connection bean leak under certain race conditions.
        factory.unregisterConnection(this);

        // if this is not in cnxns then it's already closed
        if (!factory.cnxns.remove(this)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("cnxns size:{}", factory.cnxns.size());
            }
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("close in progress for sessionid:0x{}",
                    Long.toHexString(sessionId));
        }

        factory.removeCnxnFromIpMap(
                this,
                ((InetSocketAddress)channel.remoteAddress()).getAddress());

        if (zkServer != null) {
            zkServer.removeCnxn(this);
        }

        if (channel.isOpen()) {
            // Since we don't check on the futures created by write calls to the channel complete we need to make sure
            // that all writes have been completed before closing the channel or we risk data loss
            // See: http://lists.jboss.org/pipermail/netty-users/2009-August/001122.html
            channel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                    future.channel().close().addListener(f -> releaseQueuedBuffer());
                }
            });
        } else {
            channel.eventLoop().execute(this::releaseQueuedBuffer);
        }
    }

    @Override
    public long getSessionId() {
        return sessionId;
    }

    @Override
    public int getSessionTimeout() {
        return sessionTimeout;
    }

    //首先创建ReplyHeader，然后再调用sendResponse来发送响应，最后调用close函数进行后续关闭处理
    @Override
    public void process(WatchedEvent event) {
        // 创建响应头
        ReplyHeader h = new ReplyHeader(-1, -1L, 0);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.EVENT_DELIVERY_TRACE_MASK,
                                     "Deliver event " + event + " to 0x"
                                     + Long.toHexString(this.sessionId)
                                     + " through " + this);
        }

        // Convert WatchedEvent to a type that can be sent over the wire
        WatcherEvent e = event.getWrapper();

        try {
            // 发送响应
            //--将WatcherEvent事件发给客户端，客户端通过ClinetCnxn接受并向上处理这个WatcherEvent
            //--服务端向客户端发送notification消息，消息内容是WatcherEvent
            sendResponse(h, e, "notification");
        } catch (IOException e1) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Problem sending to " + getRemoteSocketAddress(), e1);
            }
            close();
        }
    }

    @Override
    public void sendResponse(ReplyHeader h, Record r, String tag)
            throws IOException {
        if (closingChannel || !channel.isOpen()) {
            return;
        }
        super.sendResponse(h, r, tag);
        if (h.getXid() > 0) {
            // zks cannot be null otherwise we would not have gotten here!
            if (!zkServer.shouldThrottle(outstandingCount.decrementAndGet())) {
                enableRecv();
            }
        }
    }

    @Override
    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
    }

    // Use a single listener instance to reduce GC
    private final GenericFutureListener<Future<Void>> onSendBufferDoneListener = f -> {
        if (f.isSuccess()) {
            packetSent();
        }
    };

    //sendBuffer完成的操作是将ByteBuffer写入ChannelBuffer中
    @Override
    public void sendBuffer(ByteBuffer sendBuffer) {
        if (sendBuffer == ServerCnxnFactory.closeConn) {
            close();
            return;
        }
        channel.writeAndFlush(Unpooled.wrappedBuffer(sendBuffer)).addListener(onSendBufferDoneListener);
    }

    /**
     * This class wraps the sendBuffer method of NIOServerCnxn. It is
     * responsible for chunking up the response to a client. Rather
     * than cons'ing up a response fully in memory, which may be large
     * for some commands, this class chunks up the result.
     */
    //跟NIOServerCnxn一样
    private class SendBufferWriter extends Writer {
        private StringBuffer sb = new StringBuffer();

        /**
         * Check if we are ready to send another chunk.
         * @param force force sending, even if not a full chunk
         */
        // 是否准备好发送另一块
        private void checkFlush(boolean force) {
            if ((force && sb.length() > 0) || sb.length() > 2048) {// 当强制发送并且sb大小大于0，或者sb大小大于2048即发送缓存
                sendBuffer(ByteBuffer.wrap(sb.toString().getBytes()));
                // clear our internal buffer
                sb.setLength(0);
            }
        }

        @Override
        public void close() throws IOException {
            if (sb == null) return;
            // 关闭之前需要强制性发送缓存
            checkFlush(true);
            sb = null; // clear out the ref to ensure no reuse
        }

        @Override
        public void flush() throws IOException {
            checkFlush(true);
        }

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            sb.append(cbuf, off, len);
            checkFlush(false);
        }
    }

    /** Return if four letter word found and responded to, otw false **/
    private boolean checkFourLetterWord(final Channel channel, ByteBuf message, final int len) {
        // We take advantage of the limited size of the length to look
        // for cmds. They are all 4-bytes which fits inside of an int
        if (!FourLetterCommands.isKnown(len)) {
            return false;
        }

        String cmd = FourLetterCommands.getCommandString(len);

        // Stops automatic reads of incoming data on this channel. We don't
        // expect any more traffic from the client when processing a 4LW
        // so this shouldn't break anything.
        channel.config().setAutoRead(false);
        packetReceived();

        final PrintWriter pwriter = new PrintWriter(
                new BufferedWriter(new SendBufferWriter()));

        // ZOOKEEPER-2693: don't execute 4lw if it's not enabled.
        if (!FourLetterCommands.isEnabled(cmd)) {
            LOG.debug("Command {} is not executed because it is not in the whitelist.", cmd);
            NopCommand nopCmd = new NopCommand(pwriter, this, cmd +
                    " is not executed because it is not in the whitelist.");
            nopCmd.start();
            return true;
        }

        LOG.info("Processing {} command from {}", cmd, channel.remoteAddress());

       if (len == FourLetterCommands.setTraceMaskCmd) {
            ByteBuffer mask = ByteBuffer.allocate(8);
            message.readBytes(mask);
            mask.flip();
            long traceMask = mask.getLong();
            ZooTrace.setTextTraceLevel(traceMask);
            SetTraceMaskCommand setMask = new SetTraceMaskCommand(pwriter, this, traceMask);
            setMask.start();
            return true;
        } else {
            CommandExecutor commandExecutor = new CommandExecutor();
            return commandExecutor.execute(this, pwriter, len, zkServer,factory);
        }
    }

    /**
     * Helper that throws an IllegalStateException if the current thread is not
     * executing in the channel's event loop thread.
     * @param callerMethodName the name of the calling method to add to the exception message.
     */
    private void checkIsInEventLoop(String callerMethodName) {
        if (!channel.eventLoop().inEventLoop()) {
            throw new IllegalStateException(
                    callerMethodName + "() called from non-EventLoop thread");
        }
    }

    /**
     * Appends <code>buf</code> to <code>queuedBuffer</code>. Does not duplicate <code>buf</code>
     * or call any flavor of {@link ByteBuf#retain()}. Caller must ensure that <code>buf</code>
     * is not owned by anyone else, as this call transfers ownership of <code>buf</code> to the
     * <code>queuedBuffer</code>.
     *
     * This method should only be called from the event loop thread.
     * @param buf the buffer to append to the queue.
     */
    private void appendToQueuedBuffer(ByteBuf buf) {
        checkIsInEventLoop("appendToQueuedBuffer");
        if (queuedBuffer.numComponents() == queuedBuffer.maxNumComponents()) {
            // queuedBuffer has reached its component limit, so combine the existing components.
            queuedBuffer.consolidate();
        }
        queuedBuffer.addComponent(true, buf);
    }

    /**
     * Process incoming message. This should only be called from the event
     * loop thread.
     * Note that this method does not call <code>buf.release()</code>. The caller
     * is responsible for making sure the buf is released after this method
     * returns.
     * @param buf the message bytes to process.
     */
    void processMessage(ByteBuf buf) {
        checkIsInEventLoop("processMessage");
        if (LOG.isDebugEnabled()) {
            LOG.debug("0x{} queuedBuffer: {}",
                    Long.toHexString(sessionId),
                    queuedBuffer);
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("0x{} buf {}",
                    Long.toHexString(sessionId),
                    ByteBufUtil.hexDump(buf));
        }

        if (throttled.get()) {
            LOG.debug("Received message while throttled");
            // we are throttled, so we need to queue
            if (queuedBuffer == null) {
                LOG.debug("allocating queue");
                queuedBuffer = channel.alloc().compositeBuffer();
            }
            appendToQueuedBuffer(buf.retainedDuplicate());
            if (LOG.isTraceEnabled()) {
                LOG.trace("0x{} queuedBuffer {}",
                        Long.toHexString(sessionId),
                        ByteBufUtil.hexDump(queuedBuffer));
            }
        } else {
            LOG.debug("not throttled");
            if (queuedBuffer != null) {
                appendToQueuedBuffer(buf.retainedDuplicate());
                processQueuedBuffer();
            } else {
                receiveMessage(buf);
                // Have to check !closingChannel, because an error in
                // receiveMessage() could have led to close() being called.
                if (!closingChannel && buf.isReadable()) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Before copy {}", buf);
                    }
                    if (queuedBuffer == null) {
                        queuedBuffer = channel.alloc().compositeBuffer();
                    }
                    appendToQueuedBuffer(buf.retainedSlice(buf.readerIndex(), buf.readableBytes()));
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Copy is {}", queuedBuffer);
                        LOG.trace("0x{} queuedBuffer {}",
                                Long.toHexString(sessionId),
                                ByteBufUtil.hexDump(queuedBuffer));
                    }
                }
            }
        }
    }

    /**
     * Try to process previously queued message. This should only be called
     * from the event loop thread.
     */
    void processQueuedBuffer() {
        checkIsInEventLoop("processQueuedBuffer");
        if (queuedBuffer != null) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("processing queue 0x{} queuedBuffer {}",
                        Long.toHexString(sessionId),
                        ByteBufUtil.hexDump(queuedBuffer));
            }
            receiveMessage(queuedBuffer);
            if (closingChannel) {
                // close() could have been called if receiveMessage() failed
                LOG.debug("Processed queue - channel closed, dropping remaining bytes");
            } else if (!queuedBuffer.isReadable()) {
                LOG.debug("Processed queue - no bytes remaining");
                releaseQueuedBuffer();
            } else {
                LOG.debug("Processed queue - bytes remaining");
                // Try to reduce memory consumption by freeing up buffer space
                // which is no longer needed.
                queuedBuffer.discardReadComponents();
            }
        } else {
            LOG.debug("queue empty");
        }
    }

    /**
     * Clean up queued buffer once it's no longer needed. This should only be
     * called from the event loop thread.
     */
    private void releaseQueuedBuffer() {
        checkIsInEventLoop("releaseQueuedBuffer");
        if (queuedBuffer != null) {
            queuedBuffer.release();
            queuedBuffer = null;
        }
    }

    /**
     * Receive a message, which can come from the queued buffer or from a new
     * buffer coming in over the channel. This should only be called from the
     * event loop thread.
     * Note that this method does not call <code>message.release()</code>. The
     * caller is responsible for making sure the message is released after this
     * method returns.
     * @param message the message bytes to process.
     */
    //--socket接收到zookeeper客户端的请求，触发该方法，调用ZookeeperServer的ProcessPacket
    private void receiveMessage(ByteBuf message) {
        checkIsInEventLoop("receiveMessage");
        try {
            while(message.isReadable() && !throttled.get()) {//当writerIndex > readerIndex，并且不节流时，满足条件
                if (bb != null) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("message readable {} bb len {} {}",
                                message.readableBytes(),
                                bb.remaining(),
                                bb);
                        ByteBuffer dat = bb.duplicate();
                        dat.flip();
                        LOG.trace("0x{} bb {}",
                                Long.toHexString(sessionId),
                                ByteBufUtil.hexDump(Unpooled.wrappedBuffer(dat)));
                    }

                    if (bb.remaining() > message.readableBytes()) {// bb剩余空间大于message中可读字节大小
                        // 确定新的limit
                        int newLimit = bb.position() + message.readableBytes();
                        bb.limit(newLimit);
                    }
                    // 将message写入bb中
                    message.readBytes(bb);
                    // 重置bb的limit
                    bb.limit(bb.capacity());

                    if (LOG.isTraceEnabled()) {
                        LOG.trace("after readBytes message readable {} bb len {} {}",
                                message.readableBytes(),
                                bb.remaining(),
                                bb);
                        ByteBuffer dat = bb.duplicate();
                        dat.flip();
                        LOG.trace("after readbytes 0x{} bb {}",
                                Long.toHexString(sessionId),
                                ByteBufUtil.hexDump(Unpooled.wrappedBuffer(dat)));
                    }
                    // 已经读完message，表示内容已经全部接收
                    if (bb.remaining() == 0) {
                        packetReceived();// 统计接收信息
                        bb.flip();// 翻转，可读

                        ZooKeeperServer zks = this.zkServer;
                        if (zks == null || !zks.isRunning()) {
                            throw new IOException("ZK down");
                        }
                        //--客户端第一次连接，
                        if (initialized) {// 未被初始化
                            // TODO: if zks.processPacket() is changed to take a ByteBuffer[],
                            // we could implement zero-copy queueing.
                            zks.processPacket(this, bb);// 处理bb中包含的包信息

                            if (zks.shouldThrottle(outstandingCount.incrementAndGet())) {// 是否已经节流
                                disableRecvNoWait();// 不接收数据
                            }
                        } else {//--否则调用ZookeeperServerprocessConnectRequest
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("got conn req request from {}",
                                        getRemoteSocketAddress());
                            }
                            // 处理连接请求
                            zks.processConnectRequest(this, bb);
                            initialized = true;
                        }
                        bb = null;
                    }
                } else {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("message readable {} bblenrem {}",
                                message.readableBytes(),
                                bbLen.remaining());
                        // 复制bbLen缓冲
                        ByteBuffer dat = bbLen.duplicate();
                        // 翻转
                        dat.flip();
                        LOG.trace("0x{} bbLen {}",
                                Long.toHexString(sessionId),
                                ByteBufUtil.hexDump(Unpooled.wrappedBuffer(dat)));
                    }

                    if (message.readableBytes() < bbLen.remaining()) {
                        bbLen.limit(bbLen.position() + message.readableBytes());
                    }
                    message.readBytes(bbLen);
                    bbLen.limit(bbLen.capacity());
                    if (bbLen.remaining() == 0) {// 已经读完message，表示内容已经全部接收
                        // 翻转
                        bbLen.flip();

                        if (LOG.isTraceEnabled()) {
                            LOG.trace("0x{} bbLen {}",
                                    Long.toHexString(sessionId),
                                    ByteBufUtil.hexDump(Unpooled.wrappedBuffer(bbLen)));
                        }
                        // 读取position后四个字节
                        int len = bbLen.getInt();
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("0x{} bbLen len is {}",
                                    Long.toHexString(sessionId),
                                    len);
                        }
                        // 清除缓存
                        bbLen.clear();
                        if (!initialized) {
                            if (checkFourLetterWord(channel, message, len)) {
                                return;
                            }
                        }
                        if (len < 0 || len > BinaryInputArchive.maxBuffer) {
                            throw new IOException("Len error " + len);
                        }
                        // 根据len重新分配缓冲，以便接收内容
                        bb = ByteBuffer.allocate(len);
                    }
                }
            }
        } catch(IOException e) {
            LOG.warn("Closing connection to " + getRemoteSocketAddress(), e);
            close();
        }
    }

    /**
     * An event that triggers a change in the channel's "Auto Read" setting.
     * Used for throttling. By using an enum we can treat the two values as
     * singletons and compare with ==.
     */
    enum AutoReadEvent {
        DISABLE,
        ENABLE
    }

    /**
     * Note that both disableRecv() and disableRecvNoWait() are asynchronous in the netty implementation.
     */
    @Override
    public void disableRecv() {
        if (throttled.compareAndSet(false, true)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Throttling - disabling recv {}", this);
            }
            channel.pipeline().fireUserEventTriggered(AutoReadEvent.DISABLE);
        }
    }

    private void disableRecvNoWait() {
        disableRecv();
    }

    @Override
    public void enableRecv() {
        if (throttled.compareAndSet(true, false)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sending unthrottle event {}", this);
            }
            channel.pipeline().fireUserEventTriggered(AutoReadEvent.ENABLE);
        }
    }

    @Override
    public long getOutstandingRequests() {
        return outstandingCount.longValue();
    }

    @Override
    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    @Override
    public int getInterestOps() {
        // This might not be 100% right, but it's only used for printing
        // connection info in the netty implementation so it's probably ok.
        if (channel == null || !channel.isOpen()) {
            return 0;
        }
        int interestOps = 0;
        if (!throttled.get()) {
            interestOps |= SelectionKey.OP_READ;
        }
        if (!channel.isWritable()) {
            // OP_READ means "can read", but OP_WRITE means "cannot write",
            // it's weird.
            interestOps |= SelectionKey.OP_WRITE;
        }
        return interestOps;
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        return (InetSocketAddress)channel.remoteAddress();
    }

    /** Send close connection packet to the client.
     */
    @Override
    public void sendCloseSession() {
        sendBuffer(ServerCnxnFactory.closeConn);
    }

    @Override
    protected ServerStats serverStats() {
        if (zkServer == null) {
            return null;
        }
        return zkServer.serverStats();
    }

    @Override
    public boolean isSecure() {
        return factory.secure;
    }

    @Override
    public Certificate[] getClientCertificateChain() {
        if (clientChain == null)
        {
            return null;
        }
        return Arrays.copyOf(clientChain, clientChain.length);
    }

    @Override
    public void setClientCertificateChain(Certificate[] chain) {
        if (chain == null)
        {
            clientChain = null;
        } else {
            clientChain = Arrays.copyOf(chain, chain.length);
        }
    }

    // For tests and NettyServerCnxnFactory only, thus package-private.
    Channel getChannel() {
        return channel;
    }
}
