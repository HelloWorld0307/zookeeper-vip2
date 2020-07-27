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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.cert.Certificate;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.server.NIOServerCnxnFactory.SelectorThread;
import org.apache.zookeeper.server.command.CommandExecutor;
import org.apache.zookeeper.server.command.FourLetterCommands;
import org.apache.zookeeper.server.command.SetTraceMaskCommand;
import org.apache.zookeeper.server.command.NopCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ServerCnxn 代表了一个客户端与一个server的连接,其有两种实现,分别是 NIOServerCnxn和 NettyServerCnxn.
 * 当 SocketChannel 上有数据后就会调用 NIOServerCnxn 上 doIO 方法处理，NIOServerCnxn不负责处理请求，
 * 只是对请求做一些轻微校验，这里会去调用zkServer去处理数据
 *
 * NIOServerCnxn是怎么读取SocketChannel上的数据呢？
 *  1）数据的粘包拆包
 *     处理事件比较麻烦的问题就是通过TCP发送的报文会出现粘包和拆包问题，Zookeeper为了解决此问题，在设计通信协议的时候将报文分为3部分
 *     1，请求头和请求题的长度（4字节）
 *     2，请求头
 *     3，请求体
 *     注:(1)请求头和请求体也细分为更小的部分,但在此不做深入研究,只需知道请求的前4个字节是请求头和请求体的长度即可.
 *        (2)将请求头和请求体称之为payload在报文头增加了4个字节的长度字段,表示整个报文除长度字段之外的长度.服务端
 *        可根据该长度将粘包拆包的报文分离或组合为完整的报文.NIOServerCnxn读取数据流程如下:
 *          1，NIOServerCnxn中有两个属性,一个是lenBuffer,容量为4个字节,用于读取长度信息.一个是incomingBuffer,
 *          其初始化时即为lenBuffer,但是读取长度信息后,就为incomingBuffer分配对应的空间用于读取payload
 *          2，根据请求报文的长度分配incomingBuffer的大小
 *          3，将读到的字节存放在incomingBuffer中,直至读满(由于第2步中为incomingBuffer分配的长度刚好是报文的长度,此时incomingBuffer中刚好时一个报文)
 *          4，处理报文
 * 处理步骤如下：
 *
 * This class handles communication with clients using NIO. There is one per
 * client, but only one thread doing the communication.
 */
public class NIOServerCnxn extends ServerCnxn {
    private static final Logger LOG = LoggerFactory.getLogger(NIOServerCnxn.class);

    private final NIOServerCnxnFactory factory;

    private final SocketChannel sock;

    private final SelectorThread selectorThread;

    private final SelectionKey sk;

    private boolean initialized;

    private final ByteBuffer lenBuffer = ByteBuffer.allocate(4);

    private ByteBuffer incomingBuffer = lenBuffer;

    private final Queue<ByteBuffer> outgoingBuffers =
        new LinkedBlockingQueue<ByteBuffer>();

    private int sessionTimeout;

    // 整个系统只有一个zkServer，这里的zkServer只是表示为NIOServerCnxn内部的一个属性，所有客户端的NIOServerCnxn都是公用的同一个zkServer
    private final ZooKeeperServer zkServer;

    /**
     * The number of requests that have been submitted but not yet responded to.
     */
    private final AtomicInteger outstandingRequests = new AtomicInteger(0);

    /**
     * This is the id that uniquely identifies the session of a client. Once
     * this session is no longer active, the ephemeral nodes will go away.
     */
    private long sessionId;

    private final int outstandingLimit;

    public NIOServerCnxn(ZooKeeperServer zk, SocketChannel sock,
                         SelectionKey sk, NIOServerCnxnFactory factory,
                         SelectorThread selectorThread) throws IOException {
        this.zkServer = zk;
        this.sock = sock;
        this.sk = sk;
        this.factory = factory;
        this.selectorThread = selectorThread;
        if (this.factory.login != null) {
            this.zooKeeperSaslServer = new ZooKeeperSaslServer(factory.login);
        }
        if (zk != null) {
            outstandingLimit = zk.getGlobalOutstandingLimit();
        } else {
            outstandingLimit = 1;
        }
        sock.socket().setTcpNoDelay(true);
        /* set socket linger to false, so that socket close does not block */
        sock.socket().setSoLinger(false, -1);
        InetAddress addr = ((InetSocketAddress) sock.socket()
                .getRemoteSocketAddress()).getAddress();
        authInfo.add(new Id("ip", addr.getHostAddress()));
        this.sessionTimeout = factory.sessionlessCnxnTimeout;
    }

    /* Send close connection packet to the client, doIO will eventually
     * close the underlying machinery (like socket, selectorkey, etc...)
     */
    public void sendCloseSession() {
        sendBuffer(ServerCnxnFactory.closeConn);
    }

    /**
     * send buffer without using the asynchronous
     * calls to selector and then close the socket
     * @param bb
     */
    void sendBufferSync(ByteBuffer bb) {
       try {
           /* configure socket to be blocking
            * so that we dont have to do write in
            * a tight while loop
            */
           if (bb != ServerCnxnFactory.closeConn) {
               if (sock.isOpen()) {
                   sock.configureBlocking(true);
                   sock.write(bb);
               }
               packetSent();
           }
       } catch (IOException ie) {
           LOG.error("Error sending data synchronously ", ie);
       }
    }

    /**
     * sendBuffer pushes a byte buffer onto the outgoing buffer queue for
     * asynchronous writes.
     */
    public void sendBuffer(ByteBuffer bb) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Add a buffer to outgoingBuffers, sk " + sk
                      + " is valid: " + sk.isValid());
        }
        outgoingBuffers.add(bb);
        requestInterestOpsUpdate();
    }

    /** Read the request payload (everything following the length prefix) */

    /**
     * 有两种情况会调用此方法:
     *      * 1.根据lengthBuffer的值为incomingBuffer分配空间后,此时尚未将数据从socketChannel读取至incomingBuffer中
     *      * 2.已经将数据从socketChannel中读取至incomingBuffer,且读取完毕
     *
     * @throws IOException
     * @throws InterruptedException
     */
    private void readPayload() throws IOException, InterruptedException {
        // incomingBuffer 大小就是package大小，多以判断incomingBuffer是否为空可以知道数据有没有读完
        if (incomingBuffer.remaining() != 0) { // have we read length bytes?
            //对应情况1,此时刚为incomingBuffer分配空间,incomingBuffer为空,进行一次数据读取
            //(1)若将incomingBuffer读满,则直接进行处理;
            //(2)若未将incomingBuffer读满,则说明此次发送的数据不能构成一个完整的请求,则等待下一次数据到达后调用doIo()时再次将数据
            //从socketChannel读取至incomingBuffer
            int rc = sock.read(incomingBuffer); // sock is non-blocking, so ok
            if (rc < 0) {
                throw new EndOfStreamException(
                        "Unable to read additional data from client sessionid 0x"
                        + Long.toHexString(sessionId)
                        + ", likely client has closed socket");
            }
        }

        if (incomingBuffer.remaining() == 0) {
            packetReceived();
            incomingBuffer.flip();
            // 是否初始化好了
            if (!initialized) {
                // socket连接建立好了，但是还没有初始化，处理connectRequest
                readConnectRequest();
            } else {
                // ***** 通过调用zkServer去处理增删改查请求 *****
                readRequest();
            }
            lenBuffer.clear();
            incomingBuffer = lenBuffer;
        }
    }

    /**
     * This boolean tracks whether the connection is ready for selection or
     * not. A connection is marked as not ready for selection while it is
     * processing an IO request. The flag is used to gatekeep pushing interest
     * op updates onto the selector.
     */
    private final AtomicBoolean selectable = new AtomicBoolean(true);

    public boolean isSelectable() {
        return sk.isValid() && selectable.get();
    }

    public void disableSelectable() {
        selectable.set(false);
    }

    public void enableSelectable() {
        selectable.set(true);
    }

    private void requestInterestOpsUpdate() {
        if (isSelectable()) {
            selectorThread.addInterestOpsUpdateRequest(sk);
        }
    }

    void handleWrite(SelectionKey k) throws IOException, CloseRequestException {
        if (outgoingBuffers.isEmpty()) {
            return;
        }

        /*
         * This is going to reset the buffer position to 0 and the
         * limit to the size of the buffer, so that we can fill it
         * with data from the non-direct buffers that we need to
         * send.
         */
        ByteBuffer directBuffer = NIOServerCnxnFactory.getDirectBuffer();
        if (directBuffer == null) {
            ByteBuffer[] bufferList = new ByteBuffer[outgoingBuffers.size()];
            // Use gathered write call. This updates the positions of the
            // byte buffers to reflect the bytes that were written out.
            sock.write(outgoingBuffers.toArray(bufferList));

            // Remove the buffers that we have sent
            ByteBuffer bb;
            while ((bb = outgoingBuffers.peek()) != null) {
                if (bb == ServerCnxnFactory.closeConn) {
                    throw new CloseRequestException("close requested");
                }
                if (bb.remaining() > 0) {
                    break;
                }
                packetSent();
                outgoingBuffers.remove();
            }
         } else {
            directBuffer.clear();

            for (ByteBuffer b : outgoingBuffers) {
                if (directBuffer.remaining() < b.remaining()) {
                    /*
                     * When we call put later, if the directBuffer is to
                     * small to hold everything, nothing will be copied,
                     * so we've got to slice the buffer if it's too big.
                     */
                    b = (ByteBuffer) b.slice().limit(
                            directBuffer.remaining());
                }
                /*
                 * put() is going to modify the positions of both
                 * buffers, put we don't want to change the position of
                 * the source buffers (we'll do that after the send, if
                 * needed), so we save and reset the position after the
                 * copy
                 */
                int p = b.position();
                directBuffer.put(b);
                b.position(p);
                if (directBuffer.remaining() == 0) {
                    break;
                }
            }
            /*
             * Do the flip: limit becomes position, position gets set to
             * 0. This sets us up for the write.
             */
            directBuffer.flip();

            int sent = sock.write(directBuffer);

            ByteBuffer bb;

            // Remove the buffers that we have sent
            while ((bb = outgoingBuffers.peek()) != null) {
                if (bb == ServerCnxnFactory.closeConn) {
                    throw new CloseRequestException("close requested");
                }
                if (sent < bb.remaining()) {
                    /*
                     * We only partially sent this buffer, so we update
                     * the position and exit the loop.
                     */
                    bb.position(bb.position() + sent);
                    break;
                }
                packetSent();
                /* We've sent the whole buffer, so drop the buffer */
                sent -= bb.remaining();
                outgoingBuffers.remove();
            }
        }
    }

    /**
     * Only used in order to allow testing
     */
    protected boolean isSocketOpen() {
        return sock.isOpen();
    }

    /**
     * 处理读写请求
     * 前提知识：客户端与服务端交流是通过socket方式，传递命令的字节流，其中这些字节流被封装成了一个Package对象，这个对象的开头四字节（int）用来
     * 表示该Package的长度，首先服务端是先读取默认4字节长度数据，来获得该次命令的总长度，然后清空缓存；接着读取package数据执行命令
     */
    void doIO(SelectionKey k) throws InterruptedException {
        try {
            /*
                处理读操作的流程
                1.最开始incomingBuffer就是lenBuffer,容量为4.第一次读取4个字节,即此次请求报文的长度
                2.根据请求报文的长度分配incomingBuffer的大小
                3.将读到的字节存放在incomingBuffer中,直至读满
                 (由于第2步中为incomingBuffer分配的长度刚好是报文的长度,此时incomingBuffer中刚好时一个报文)
                4.处理报文
            */

            // 当前的Socket是否还在连接
            if (isSocketOpen() == false) {
                LOG.warn("trying to do i/o on a null socket for session:0x"
                         + Long.toHexString(sessionId));

                return;
            }
            if (k.isReadable()) {
                // 读就绪，把数据读到incomingBuffer中，incomingBuffer一开始默认大小事4字节
                int rc = sock.read(incomingBuffer); // 读取4个字节，存放到incomingBuffer中，该package的前4个字节是用来记录文件大小
                if (rc < 0) {
                    throw new EndOfStreamException(
                            "Unable to read additional data from client sessionid 0x"
                            + Long.toHexString(sessionId)
                            + ", likely client has closed socket");
                }
                /*
                    只有incomingBuffer.remaining() == 0,才会进行下一步的处理,否则一直读取数据直到incomingBuffer读满,此时有两种可能:
                    1.incomingBuffer就是lenBuffer,此时incomingBuffer的内容是此次请求报文的长度.
                     根据lenBuffer为incomingBuffer分配空间后调用readPayload().
                     在readPayload()中会立马进行一次数据读取,(1)若可以将incomingBuffer读满,则incomingBuffer中就是一个完整的请求,处理该请求;
                     (2)若不能将incomingBuffer读满,说明出现了拆包问题,此时不能构造一个完整的请求,只能等待客户端继续发送数据,
                     等到下次socketChannel可读时,继续将数据读取到incomingBuffer中
                    2.incomingBuffer不是lenBuffer,说明上次读取时出现了拆包问题,incomingBuffer中只有一个请求的部分数据.
                    而这次读取的数据加上上次读取的数据凑成了一个完整的请求,调用readPayload()
                 */
                if (incomingBuffer.remaining() == 0) {
                    boolean isPayload;
                    // 这一步的作用是清除第一次获取package长度的缓存数据，第一次只是获取其长度，保存在缓存区中的数据是没有用，
                    if (incomingBuffer == lenBuffer) { // start of next request
                        incomingBuffer.flip();
                        // isPayload = false表示为第一次读取package前面的4字节，需要重新继续读
                        isPayload = readLength(k);
                        incomingBuffer.clear();
                    } else {
                        // continuation
                        isPayload = true;
                    }
                    // 到这里说明已经读取了真正的客户端命令数据，接着执行
                    if (isPayload) { // not the case for 4letterword
                        // 读取客户端命令Package，并调用zkServer执行命令
                        readPayload();
                    }
                    else {
                        // four letter words take care
                        // need not do anything else
                        return;
                    }
                }
            }
            if (k.isWritable()) {
                handleWrite(k);

                if (!initialized && !getReadInterest() && !getWriteInterest()) {
                    throw new CloseRequestException("responded to info probe");
                }
            }
        } catch (CancelledKeyException e) {
            LOG.warn("CancelledKeyException causing close of session 0x"
                     + Long.toHexString(sessionId));
            if (LOG.isDebugEnabled()) {
                LOG.debug("CancelledKeyException stack trace", e);
            }
            close();
        } catch (CloseRequestException e) {
            // expecting close to log session closure
            close();
        } catch (EndOfStreamException e) {
            LOG.warn(e.getMessage());
            // expecting close to log session closure
            close();
        } catch (IOException e) {
            LOG.warn("Exception causing close of session 0x"
                     + Long.toHexString(sessionId) + ": " + e.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug("IOException stack trace", e);
            }
            close();
        }
    }

    /**
     * NIOServerCnxn不负责处理请求，只是对请求做一些轻微校验，这里会去调用zkServer去处理数据。
     */
    private void readRequest() throws IOException {
        zkServer.processPacket(this, incomingBuffer);
    }

    // Only called as callback from zkServer.processPacket()
    protected void incrOutstandingRequests(RequestHeader h) {
        if (h.getXid() >= 0) {
            outstandingRequests.incrementAndGet();
            // check throttling
            int inProcess = zkServer.getInProcess();
            if (inProcess > outstandingLimit) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Throttling recv " + inProcess);
                }
                disableRecv();

            }
        }
    }

    // returns whether we are interested in writing, which is determined
    // by whether we have any pending buffers on the output queue or not
    private boolean getWriteInterest() {
        return !outgoingBuffers.isEmpty();
    }

    // returns whether we are interested in taking new requests, which is
    // determined by whether we are currently throttled or not
    private boolean getReadInterest() {
        return !throttled.get();
    }

    private final AtomicBoolean throttled = new AtomicBoolean(false);

    // Throttle acceptance of new requests. If this entailed a state change,
    // register an interest op update request with the selector.
    public void disableRecv() {
        if (throttled.compareAndSet(false, true)) {
            requestInterestOpsUpdate();
        }
    }

    // Disable throttling and resume acceptance of new requests. If this
    // entailed a state change, register an interest op update request with
    // the selector.
    public void enableRecv() {
        if (throttled.compareAndSet(true, false)) {
            requestInterestOpsUpdate();
        }
    }

    private void readConnectRequest() throws IOException, InterruptedException {
        if (!isZKServerRunning()) {
            throw new IOException("ZooKeeperServer not running");
        }
        zkServer.processConnectRequest(this, incomingBuffer);
        initialized = true;
    }

    /**
     * This class wraps the sendBuffer method of NIOServerCnxn. It is
     * responsible for chunking up the response to a client. Rather
     * than cons'ing up a response fully in memory, which may be large
     * for some commands, this class chunks up the result.
     */
    private class SendBufferWriter extends Writer {
        private StringBuffer sb = new StringBuffer();

        /**
         * Check if we are ready to send another chunk.
         * @param force force sending, even if not a full chunk
         */
        private void checkFlush(boolean force) {
            if ((force && sb.length() > 0) || sb.length() > 2048) {
                sendBufferSync(ByteBuffer.wrap(sb.toString().getBytes()));
                // clear our internal buffer
                sb.setLength(0);
            }
        }

        @Override
        public void close() throws IOException {
            if (sb == null) return;
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
    private boolean checkFourLetterWord(final SelectionKey k, final int len)
    throws IOException
    {
        // We take advantage of the limited size of the length to look
        // for cmds. They are all 4-bytes which fits inside of an int
        if (!FourLetterCommands.isKnown(len)) {
            return false;
        }

        String cmd = FourLetterCommands.getCommandString(len);
        packetReceived();

        /** cancel the selection key to remove the socket handling
         * from selector. This is to prevent netcat problem wherein
         * netcat immediately closes the sending side after sending the
         * commands and still keeps the receiving channel open.
         * The idea is to remove the selectionkey from the selector
         * so that the selector does not notice the closed read on the
         * socket channel and keep the socket alive to write the data to
         * and makes sure to close the socket after its done writing the data
         */
        if (k != null) {
            try {
                k.cancel();
            } catch(Exception e) {
                LOG.error("Error cancelling command selection key ", e);
            }
        }

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

        LOG.info("Processing " + cmd + " command from "
                + sock.socket().getRemoteSocketAddress());

        if (len == FourLetterCommands.setTraceMaskCmd) {
            incomingBuffer = ByteBuffer.allocate(8);
            int rc = sock.read(incomingBuffer);
            if (rc < 0) {
                throw new IOException("Read error");
            }
            incomingBuffer.flip();
            long traceMask = incomingBuffer.getLong();
            ZooTrace.setTextTraceLevel(traceMask);
            SetTraceMaskCommand setMask = new SetTraceMaskCommand(pwriter, this, traceMask);
            setMask.start();
            return true;
        } else {
            CommandExecutor commandExecutor = new CommandExecutor();
            return commandExecutor.execute(this, pwriter, len, zkServer, factory);
        }
    }

    /**
     *  功能：根据读取到的package包前4位预留字段具体数值，在系统中重新开辟一个package自身大小的缓存，供下面步骤读取具体package数据
     *
     * @param k selection key
     * @return true if length read, otw false (wasn't really the length)
     * @throws IOException if buffer size exceeds maxBuffer size
     */
    private boolean readLength(SelectionKey k) throws IOException {
        // Read the length, now get the buffer
        int len = lenBuffer.getInt();
        // 判断是否为第一次读取数据 && 是否为4字节长度--因为package的前4个字节是预留字段表示package数据整体长度，所以第一次读取是一个package数据包长度值
        if (!initialized && checkFourLetterWord(sk, len)) {
            return false;
        }
        if (len < 0 || len > BinaryInputArchive.maxBuffer) {
            throw new IOException("Len error " + len);
        }
        if (!isZKServerRunning()) {
            throw new IOException("ZooKeeperServer not running");
        }
        // 当前 len 为package具体长度，根据此len开辟一个具体缓冲区
        incomingBuffer = ByteBuffer.allocate(len);
        return true;
    }

    /**
     * @return true if the server is running, false otherwise.
     */
    boolean isZKServerRunning() {
        return zkServer != null && zkServer.isRunning();
    }

    public long getOutstandingRequests() {
        return outstandingRequests.get();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.server.ServerCnxnIface#getSessionTimeout()
     */
    public int getSessionTimeout() {
        return sessionTimeout;
    }

    /**
     * Used by "dump" 4-letter command to list all connection in
     * cnxnExpiryMap
     */
    @Override
    public String toString() {
        return "ip: " + sock.socket().getRemoteSocketAddress() +
               " sessionId: 0x" + Long.toHexString(sessionId);
    }

    /**
     * Close the cnxn and remove it from the factory cnxns list.
     */
    @Override
    public void close() {
        if (!factory.removeCnxn(this)) {
            return;
        }

        if (zkServer != null) {
            zkServer.removeCnxn(this);
        }

        if (sk != null) {
            try {
                // need to cancel this selection key from the selector
                sk.cancel();
            } catch (Exception e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("ignoring exception during selectionkey cancel", e);
                }
            }
        }

        closeSock();
    }

    /**
     * Close resources associated with the sock of this cnxn.
     */
    private void closeSock() {
        if (sock.isOpen() == false) {
            return;
        }

        LOG.debug("Closed socket connection for client "
                + sock.socket().getRemoteSocketAddress()
                + (sessionId != 0 ?
                        " which had sessionid 0x" + Long.toHexString(sessionId) :
                        " (no session established for client)"));
        closeSock(sock);
    }

    /**
     * Close resources associated with a sock.
     */
    public static void closeSock(SocketChannel sock) {
        if (sock.isOpen() == false) {
            return;
        }

        try {
            /*
             * The following sequence of code is stupid! You would think that
             * only sock.close() is needed, but alas, it doesn't work that way.
             * If you just do sock.close() there are cases where the socket
             * doesn't actually close...
             */
            sock.socket().shutdownOutput();
        } catch (IOException e) {
            // This is a relatively common exception that we can't avoid
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during output shutdown", e);
            }
        }
        try {
            sock.socket().shutdownInput();
        } catch (IOException e) {
            // This is a relatively common exception that we can't avoid
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during input shutdown", e);
            }
        }
        try {
            sock.socket().close();
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during socket close", e);
            }
        }
        try {
            sock.close();
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("ignoring exception during socketchannel close", e);
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.server.ServerCnxnIface#sendResponse(org.apache.zookeeper.proto.ReplyHeader,
     *      org.apache.jute.Record, java.lang.String)
     */
    @Override
    public void sendResponse(ReplyHeader h, Record r, String tag) {
        try {
            super.sendResponse(h, r, tag);
            if (h.getXid() > 0) {
                // check throttling
                if (outstandingRequests.decrementAndGet() < 1 ||
                    zkServer.getInProcess() < outstandingLimit) {
                    enableRecv();
                }
            }
         } catch(Exception e) {
            LOG.warn("Unexpected exception. Destruction averted.", e);
         }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.server.ServerCnxnIface#process(org.apache.zookeeper.proto.WatcherEvent)
     */
    @Override
    public void process(WatchedEvent event) {
        ReplyHeader h = new ReplyHeader(-1, -1L, 0);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.EVENT_DELIVERY_TRACE_MASK,
                                     "Deliver event " + event + " to 0x"
                                     + Long.toHexString(this.sessionId)
                                     + " through " + this);
        }

        // Convert WatchedEvent to a type that can be sent over the wire
        WatcherEvent e = event.getWrapper();

        sendResponse(h, e, "notification");
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.zookeeper.server.ServerCnxnIface#getSessionId()
     */
    @Override
    public long getSessionId() {
        return sessionId;
    }

    @Override
    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
        factory.addSession(sessionId, this);
    }

    @Override
    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
        factory.touchCnxn(this);
    }

    @Override
    public int getInterestOps() {
        if (!isSelectable()) {
            return 0;
        }
        int interestOps = 0;
        if (getReadInterest()) {
            interestOps |= SelectionKey.OP_READ;
        }
        if (getWriteInterest()) {
            interestOps |= SelectionKey.OP_WRITE;
        }
        return interestOps;
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        if (sock.isOpen() == false) {
            return null;
        }
        return (InetSocketAddress) sock.socket().getRemoteSocketAddress();
    }

    public InetAddress getSocketAddress() {
        if (sock.isOpen() == false) {
            return null;
        }
        return sock.socket().getInetAddress();
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
        return false;
    }

    @Override
    public Certificate[] getClientCertificateChain() {
        throw new UnsupportedOperationException(
                "SSL is unsupported in NIOServerCnxn");
    }

    @Override
    public void setClientCertificateChain(Certificate[] chain) {
        throw new UnsupportedOperationException(
                "SSL is unsupported in NIOServerCnxn");
    }

}
