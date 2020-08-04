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

package org.apache.zookeeper.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 1、Socket、SocketChannel有什么区别?
 *  Socket、SocketChannel二者的实质都是一样的，都是为了实现客户端与服务器端的连接而存在的，但是在使用上，却有很大的区别。具体如下：
 *
 * 1)所属包不同:
 *  Socket在java.net包中，而SocketChannel在java.nio包中。
 *
 * 2)异步方式不同:
 *  从包的不同，我们大体可以推断出他们主要的区别：Socket是阻塞连接（当然我们可以自己实现非阻塞），SocketChannel可以设置非阻塞连接。
 *  使用ServerSocket、Socket类时，服务端Socket往往要为每一个客户端Socket分配一个线程，而每一个线程都有可能处于长时间的阻塞状态中。
 *  过多的线程也会影响服务器的性能（可以使用线程池优化，具体看这里：如何编写多线程Socket程序）。而使用SocketChannel、ServerSocketChannel类
 *  可以非阻塞通信，这样使得服务器端只需要一个线程就能处理所有客户端socket的请求。
 *
 *  了解阻塞、非阻塞看这里：[阻塞、非阻塞有什么区别][3]
 *
 * 3)性能不同:
 *  一般来说使用SocketChannel会有更好的性能。其实，Socket实际应该比SocketChannel更高效，不过由于使用者设计等原因，效率反而比直接使用SocketChannel低。
 *
 * 4)使用方式不同:
 *  Socket、ServerSocket类可以传入不同参数直接实例化对象并绑定ip和端口，实例代码如：
 *      Socket socket = new Socket("127.0.0.1", "8000");
 *      ServerSocket serverSocket = new ServerSocket("8000");
 *
 *  而SocketChannel、ServerSocketChannel类需要借助Selector类控制，实例代码如：
 *      Selector selector = Selector.open();
 *      ServerSocketChannel serverChannel = ServerSocketChannel.open();
 *      serverChannel.configureBlocking(false); // 设置为非阻塞方式,如果为true 那么就为传统的阻塞方式
 *      serverChannel.socket().bind(new InetSocketAddress(port)); // 绑定IP 及 端口
 *      serverChannel.register(selector, SelectionKey.OP_ACCEPT); // 注册 OP_ACCEPT事件
 *      new ServerThread().start(); // 开启一个线程 处理所有请求
 *
 *
 *
 * 2、SocketChannel方式有什么核心类?
 * 下面是SocketChannel方式需要用到的几个核心类：
 *
 *  1),ServerSocketChannel
 *     ServerSocket的替代类, 支持阻塞通信与非阻塞通信。
 *
 *  2),SocketChannel
 *     Socket的替代类, 支持阻塞通信与非阻塞通信。
 *
 *  3),Selector
 *     为ServerSocketChannel监控接收客户端连接就绪事件, 为SocketChannel监控连接服务器读就绪和写就绪事件。
 *
 *  4),SelectionKey
 *      代表ServerSocketChannel及SocketChannel向Selector注册事件的句柄。当一个
 *      SelectionKey对象位于Selector对象的selected-keys集合中时，就表示与这个SelectionKey对象相关的事件发生了。
 *      在SelectionKey类中有几个静态常量：
 *      SelectionKey.OP_ACCEPT，客户端连接就绪事件，等于监听serversocket.accept()，返回一个socket。
 *      SelectionKey.OP_CONNECT，准备连接服务器就绪，跟上面类似，只不过是对于socket的 相当于监听了socket.connect()。
 *      SelectionKey.OP_READ，读就绪事件, 表示输入流中已经有了可读数据, 可以执行读操作。
 *      SelectionKey.OP_WRITE，写就绪事件, 表示可以执行写操作。
 *
 * NIOServerCnxnFactory implements a multi-threaded ServerCnxnFactory using
 * NIO non-blocking socket calls. Communication between threads is handled via
 * queues.
 *
 *   - 1   accept thread, which accepts new connections and assigns to a
 *         selector thread
 *   - 1-N selector threads, each of which selects on 1/N of the connections.
 *         The reason the factory supports more than one selector thread is that
 *         with large numbers of connections, select() itself can become a
 *         performance bottleneck.
 *   - 0-M socket I/O worker threads, which perform basic socket reads and
 *         writes. If configured with 0 worker threads, the selector threads
 *         do the socket I/O directly.
 *   - 1   connection expiration thread, which closes idle connections; this is
 *         necessary to expire connections on which no session is established.
 *
 * Typical (default) thread counts are: on a 32 core machine, 1 accept thread,
 * 1 connection expiration thread, 4 selector threads, and 64 worker threads.
 */
public class NIOServerCnxnFactory extends ServerCnxnFactory {
    private static final Logger LOG = LoggerFactory.getLogger(NIOServerCnxnFactory.class);

    /** Default sessionless connection timeout in ms: 10000 (10s) */
    public static final String ZOOKEEPER_NIO_SESSIONLESS_CNXN_TIMEOUT =
            "zookeeper.nio.sessionlessCnxnTimeout";
    /**
     * With 500 connections to an observer with watchers firing on each, is
     * unable to exceed 1GigE rates with only 1 selector.
     * Defaults to using 2 selector threads with 8 cores and 4 with 32 cores.
     * Expressed as sqrt(numCores/2). Must have at least 1 selector thread.
     */
    public static final String ZOOKEEPER_NIO_NUM_SELECTOR_THREADS =
            "zookeeper.nio.numSelectorThreads";
    /** Default: 2 * numCores */
    public static final String ZOOKEEPER_NIO_NUM_WORKER_THREADS =
            "zookeeper.nio.numWorkerThreads";
    /** Default: 64kB */
    public static final String ZOOKEEPER_NIO_DIRECT_BUFFER_BYTES =
            "zookeeper.nio.directBufferBytes";
    /** Default worker pool shutdown timeout in ms: 5000 (5s) */
    public static final String ZOOKEEPER_NIO_SHUTDOWN_TIMEOUT =
            "zookeeper.nio.shutdownTimeout";

    static {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
                LOG.error("Thread " + t + " died", e);
            }
        });
        /**
         * this is to avoid the jvm bug:
         * NullPointerException in Selector.open()
         * http://bugs.sun.com/view_bug.do?bug_id=6427854
         */
        try {
            Selector.open().close();
        } catch (IOException ie) {
            LOG.error("Selector failed to open", ie);
        }

        /**
         * Value of 0 disables use of direct buffers and instead uses
         * gathered write call.
         *
         * Default to using 64k direct buffers.
         */
        directBufferBytes = Integer.getInteger(
                ZOOKEEPER_NIO_DIRECT_BUFFER_BYTES, 64 * 1024);
    }

    /**
     * AbstractSelectThread is an abstract base class containing a few bits
     * of code shared by the AcceptThread (which selects on the listen socket)
     * and SelectorThread (which selects on client connections) classes.
     */
    private abstract class AbstractSelectThread extends ZooKeeperThread {
        protected final Selector selector;

        public AbstractSelectThread(String name) throws IOException {
            super(name);
            // Allows the JVM to shutdown even if this thread is still running.
            setDaemon(true);
            this.selector = Selector.open();
        }

        public void wakeupSelector() {
            selector.wakeup();
        }

        /**
         * Close the selector. This should be called when the thread is about to
         * exit and no operation is going to be performed on the Selector or
         * SelectionKey
         */
        protected void closeSelector() {
            try {
                selector.close();
            } catch (IOException e) {
                LOG.warn("ignored exception during selector close "
                        + e.getMessage());
            }
        }

        protected void cleanupSelectionKey(SelectionKey key) {
            if (key != null) {
                try {
                    key.cancel();
                } catch (Exception ex) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("ignoring exception during selectionkey cancel", ex);
                    }
                }
            }
        }

        protected void fastCloseSock(SocketChannel sc) {
            if (sc != null) {
                try {
                    // Hard close immediately, discarding buffers
                    sc.socket().setSoLinger(true, 0);
                } catch (SocketException e) {
                    LOG.warn("Unable to set socket linger to 0, socket close"
                            + " may stall in CLOSE_WAIT", e);
                }
                NIOServerCnxn.closeSock(sc);
            }
        }
    }

    /**
     * There is a single AcceptThread which accepts new connections and assigns
     * them to a SelectorThread using a simple round-robin scheme to spread
     * them across the SelectorThreads. It enforces maximum number of
     * connections per IP and attempts to cope with running out of file
     * descriptors by briefly sleeping before retrying.
     */
    private class AcceptThread extends AbstractSelectThread {
        // 一个客户端连接到服务端后会生成一个 ServerSocketChannel对象用来表示socket链接
        private final ServerSocketChannel acceptSocket;
        private final SelectionKey acceptKey;
        private final RateLogger acceptErrorLogger = new RateLogger(LOG);
        private final Collection<SelectorThread> selectorThreads;
        private Iterator<SelectorThread> selectorIterator;
        private volatile boolean reconfiguring = false;

        public AcceptThread(ServerSocketChannel ss, InetSocketAddress addr,
                            Set<SelectorThread> selectorThreads) throws IOException {
            super("NIOServerCxnFactory.AcceptThread:" + addr);
            this.acceptSocket = ss;
            this.acceptKey =
                    acceptSocket.register(selector, SelectionKey.OP_ACCEPT);
            this.selectorThreads = Collections.unmodifiableList(
                    new ArrayList<SelectorThread>(selectorThreads));
            selectorIterator = this.selectorThreads.iterator();
        }

        /**
         * 启动 AcceptThread 线程，他是怎么监听socket连接？
         *
         */
        public void run() {
            try {
                while (!stopped && !acceptSocket.socket().isClosed()) { // acceptSocket是ServerSocketChannel
                    try {
                        select();
                    } catch (RuntimeException e) {
                        LOG.warn("Ignoring unexpected runtime exception", e);
                    } catch (Exception e) {
                        LOG.warn("Ignoring unexpected exception", e);
                    }
                }
            } finally {
                closeSelector();
                // This will wake up the selector threads, and tell the
                // worker thread pool to begin shutdown.
                if (!reconfiguring) {
                    NIOServerCnxnFactory.this.stop();
                }
                LOG.info("accept thread exitted run method");
            }
        }

        public void setReconfiguring() {
            reconfiguring = true;
        }

        private void select() {
            try {

                selector.select();

                Iterator<SelectionKey> selectedKeys =
                        selector.selectedKeys().iterator();
                while (!stopped && selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    selectedKeys.remove();

                    if (!key.isValid()) {
                        continue;
                    }
                    if (key.isAcceptable()) {
                        if (!doAccept()) {
                            // If unable to pull a new connection off the accept
                            // queue, pause accepting to give us time to free
                            // up file descriptors and so the accept thread
                            // doesn't spin in a tight loop.
                            pauseAccept(10);
                        }
                    } else {
                        LOG.warn("Unexpected ops in accept select "
                                + key.readyOps());
                    }
                }
            } catch (IOException e) {
                LOG.warn("Ignoring IOException while selecting", e);
            }
        }

        /**
         * Mask off the listen socket interest ops and use select() to sleep
         * so that other threads can wake us up by calling wakeup() on the
         * selector.
         */
        private void pauseAccept(long millisecs) {
            acceptKey.interestOps(0);
            try {
                selector.select(millisecs);
            } catch (IOException e) {
                // ignore
            } finally {
                acceptKey.interestOps(SelectionKey.OP_ACCEPT);
            }
        }

        /**
         * 接收一个新的 socket 连接，按照客户端IP地址来计数客户端连接数量，轮询所有的 selector 线程，
         * 将当前连接 SocketChannel 对象加入到空闲 selector 的队列中，返回是是否加入成功状态值，遇
         * 错误后快速关闭当前 socket 连接
         * 通过 ServerSocketChannel.accept() 方法监听新进来的连接。当 accept()方法返回的时候,它返回
         * 一个包含新进来的连接的 SocketChannel。因此, accept()方法会一直阻塞到有新连接到达。
         *
         * 通常不会仅仅只监听一个连接,在while循环中调用 accept()方法.
         *
         * @return 是否成功加入到selector的AcceptedQueue中
         */
        private boolean doAccept() {
            boolean accepted = false;
            SocketChannel sc = null;
            try {
                // 通过 ServerSocketChannel.accept() 方法监听新进来的连接。当 accept()方法返回的时候,它返回
                // 一个包含新进来的连接的 SocketChannel。因此, accept()方法会一直阻塞到有新连接到达。
                // 通常不会仅仅只监听一个连接,在while循环中调用 accept()方法. 该while循环在doAccept方法上一层
                sc = acceptSocket.accept();
                accepted = true;
                InetAddress ia = sc.socket().getInetAddress();
                int cnxncount = getClientCnxnCount(ia);

                // 判断当前已连客户端数量
                if (maxClientCnxns > 0 && cnxncount >= maxClientCnxns) {
                    throw new IOException("Too many connections from " + ia
                            + " - max is " + maxClientCnxns);
                }

                LOG.debug("Accepted socket connection from "
                        + sc.socket().getRemoteSocketAddress());
                // 设置为非阻塞型
                sc.configureBlocking(false);

                // Round-robin assign this connection to a selector thread
                // 轮询获取当前空闲的 SelectorThread 对象---当执行到了最后一个后会继续重新从第一个继续
                if (!selectorIterator.hasNext()) {
                    selectorIterator = selectorThreads.iterator();
                }
                SelectorThread selectorThread = selectorIterator.next();

                // 将当前socket对象 SocketChannel加入到SelectorThread内部的队列中
                if (!selectorThread.addAcceptedConnection(sc)) {
                    throw new IOException(
                            "Unable to add connection to selector queue"
                                    + (stopped ? " (shutdown in progress)" : ""));
                }
                acceptErrorLogger.flush();
            } catch (IOException e) {
                // accept, maxClientCnxns, configureBlocking
                acceptErrorLogger.rateLimitLog(
                        "Error accepting new connection: " + e.getMessage());
                fastCloseSock(sc);
            }
            return accepted;
        }
    }

    /**
     * The SelectorThread receives newly accepted connections from the
     * AcceptThread and is responsible for selecting for I/O readiness
     * across the connections. This thread is the only thread that performs
     * any non-threadsafe or potentially blocking calls on the selector
     * (registering new connections and reading/writing interest ops).
     *
     * Assignment of a connection to a SelectorThread is permanent and only
     * one SelectorThread will ever interact with the connection. There are
     * 1-N SelectorThreads, with connections evenly apportioned between the
     * SelectorThreads.
     *
     * If there is a worker thread pool, when a connection has I/O to perform
     * the SelectorThread removes it from selection by clearing its interest
     * ops and schedules the I/O for processing by a worker thread. When the
     * work is complete, the connection is placed on the ready queue to have
     * its interest ops restored and resume selection.
     *
     * If there is no worker thread pool, the SelectorThread performs the I/O
     * directly.
     */
    class SelectorThread extends AbstractSelectThread {
        private final int id;
        private final Queue<SocketChannel> acceptedQueue;
        private final Queue<SelectionKey> updateQueue;

        public SelectorThread(int id) throws IOException {
            super("NIOServerCxnFactory.SelectorThread-" + id);
            this.id = id;
            acceptedQueue = new LinkedBlockingQueue<SocketChannel>();
            updateQueue = new LinkedBlockingQueue<SelectionKey>();
        }

        /**
         * Place new accepted connection onto a queue for adding. Do this
         * so only the selector thread modifies what keys are registered
         * with the selector.
         * 将客户端连接SocketChannel加入SelectorThread队列中，
         *
         * NIO中的Selector封装了底层的系统调用，其中wakeup用于唤醒阻塞在select方法上的线程，
         * 它的实现很简单，在linux上就是创建一 个管道并加入poll的fd集合，wakeup就是往管道里
         * 写一个字节，那么阻塞的poll方法有数据可读就立即返回
         * 参考：https://www.iteye.com/topic/791244
         */
        public boolean addAcceptedConnection(SocketChannel accepted) {
            if (stopped || !acceptedQueue.offer(accepted)) {
                return false;
            }
            // 通常在selector.select()后线程会阻塞。使用wakeup()方法，线程会立即返回
            wakeupSelector();
            return true;
        }

        /**
         * Place interest op update requests onto a queue so that only the
         * selector thread modifies interest ops, because interest ops
         * reads/sets are potentially blocking operations if other select
         * operations are happening.
         */
        public boolean addInterestOpsUpdateRequest(SelectionKey sk) {
            if (stopped || !updateQueue.offer(sk)) {
                return false;
            }
            wakeupSelector();
            return true;
        }

        /**
         * The main loop for the thread selects() on the connections and
         * dispatches ready I/O work requests, then registers all pending
         * newly accepted connections and updates any interest ops on the
         * queue.
         *
         *
         */
        public void run() {
            try {
                // AdminServer 如果没有修改stopped属性就会一直循环
                while (!stopped) {
                    try {
                        // 查询就绪事件
                        select();

                        // 1，这个方法会对AcceptedQueue队列中某个SocketChannel向selector上注册OP_READ事件，
                        //    只有注册了才能在下一次循环的select方法中才可以拿到就绪事件。
                        // 2，初始化一个NIOServerCnxn，因为对客户端命令的执行就是通过NIOServerCnxn里面的doIO来完成的
                        processAcceptedConnections();
                        processInterestOpsUpdateRequests();
                    } catch (RuntimeException e) {
                        LOG.warn("Ignoring unexpected runtime exception", e);
                    } catch (Exception e) {
                        LOG.warn("Ignoring unexpected exception", e);
                    }
                }

                // Close connections still pending on the selector. Any others
                // with in-flight work, let drain out of the work queue.
                for (SelectionKey key : selector.keys()) {
                    NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();
                    if (cnxn.isSelectable()) {
                        cnxn.close();
                    }
                    cleanupSelectionKey(key);
                }
                SocketChannel accepted;
                while ((accepted = acceptedQueue.poll()) != null) {
                    fastCloseSock(accepted);
                }
                updateQueue.clear();
            } finally {
                closeSelector();
                // This will wake up the accept thread and the other selector
                // threads, and tell the worker thread pool to begin shutdown.
                NIOServerCnxnFactory.this.stop();
                LOG.info("selector thread exitted run method");
            }
        }

        /**
         * 在上步processAcceptedConnections() 中会对当前SelectorThread线程的队列里面
         * 所有的SocketChannel对象进行注册，最后包装成一个SelectionKey对象保存在Set中，
         * 在该方法中则直接将这些就绪的事件拿出来处理
         */
        private void select() {
            try {
                // 这里如果阻塞了，就会导致上一层while一直阻塞，直到事件已经就绪好了才会解阻塞，但是如果selector没有注册任何事件就不会导致阻塞
                selector.select();

                // 拿到所有当前SelectorThread里面的就绪事件
                Set<SelectionKey> selected = selector.selectedKeys();
                ArrayList<SelectionKey> selectedList = new ArrayList<SelectionKey>(selected);

                // 这里会打乱就绪事件顺序
                Collections.shuffle(selectedList);

                // 遍历就绪事件
                Iterator<SelectionKey> selectedKeys = selectedList.iterator();

                // 如果当前服务端没有被AdminServer关闭 && 下一个就绪事件不为空
                while (!stopped && selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();

                    // 事件一旦处理过了，就会从就绪事件集合Set中删除掉
                    selected.remove(key);

                    if (!key.isValid()) {
                        cleanupSelectionKey(key);
                        continue;
                    }
                    // 获取读就绪与写就绪事件
                    if (key.isReadable() || key.isWritable()) {
                        // 处理客户端命令，请求
                        handleIO(key);
                    } else {
                        LOG.warn("Unexpected ops in select " + key.readyOps());
                    }
                }
            } catch (IOException e) {
                LOG.warn("Ignoring IOException while selecting", e);
            }
        }

        /**
         * Schedule I/O for processing on the connection associated with
         * the given SelectionKey. If a worker thread pool is not being used,
         * I/O is run directly by this thread.
         */
        private void handleIO(SelectionKey key) {
            // 这一步同时也会初始化一个 NIOServerCnxn
            IOWorkRequest iOWorkRequest = new IOWorkRequest(this, key);
            NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();

            // Stop selecting this key while processing on its
            // connection
            // 这里将NIOServerCnxn关闭掉，保证同一时间只能处理一个key，
            cnxn.disableSelectable();
            key.interestOps(0);
            // 更新NIOServerCnxn上的时间，NIOServerCnxn也是一个对象，如果没有接收到数据（客户端一直没有发送数据和ping）
            // 服务端就会清除长时间没有活动的客户端，所以这一步是来讲当前客户端延长连接时间的
            touchCnxn(cnxn);
            // 给线程池处理workRequest
            workerPool.schedule(iOWorkRequest);
        }

        /**
         * Iterate over the queue of accepted connections that have been
         * assigned to this thread but not yet placed on the selector.
         */
        private void processAcceptedConnections() {
            SocketChannel accepted;
            while (!stopped && (accepted = acceptedQueue.poll()) != null) {
                SelectionKey key = null;
                try {
                    // 1，注册可读事件
                    key = accepted.register(selector, SelectionKey.OP_READ);
                    // 2，新建一个NIOServerCnxn
                    NIOServerCnxn cnxn = createConnection(accepted, key, this);
                    // 3，为当前客户端socket连接刷新，防止过期
                    key.attach(cnxn);
                    addCnxn(cnxn);
                } catch (IOException e) {
                    // register, createConnection
                    cleanupSelectionKey(key);
                    fastCloseSock(accepted);
                }
            }
        }

        /**
         * Iterate over the queue of connections ready to resume selection,
         * and restore their interest ops selection mask.
         */
        private void processInterestOpsUpdateRequests() {
            SelectionKey key;
            while (!stopped && (key = updateQueue.poll()) != null) {
                if (!key.isValid()) {
                    cleanupSelectionKey(key);
                }
                NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();
                if (cnxn.isSelectable()) {
                    key.interestOps(cnxn.getInterestOps());
                }
            }
        }
    }

    /**
     * Socket上线文，对客户端命令其实是有此类的doWork()来处理的
     *
     * IOWorkRequest is a small wrapper class to allow doIO() calls to be
     * run on a connection using a WorkerService.
     */
    private class IOWorkRequest extends WorkerService.WorkRequest {
        private final SelectorThread selectorThread;
        private final SelectionKey key;
        private final NIOServerCnxn cnxn;

        IOWorkRequest(SelectorThread selectorThread, SelectionKey key) {
            this.selectorThread = selectorThread;
            this.key = key;
            this.cnxn = (NIOServerCnxn) key.attachment();
        }

        public void doWork() throws InterruptedException {
            if (!key.isValid()) {
                selectorThread.cleanupSelectionKey(key);
                return;
            }

            // 读或写就绪事件都可以触发处理IO操作
            if (key.isReadable() || key.isWritable()) {
                // 处理key，
                // 到这里，多个客户端请求还是并发处理的
                cnxn.doIO(key);

                // Check if we shutdown or doIO() closed this connection
                if (stopped) {
                    cnxn.close();
                    return;
                }
                if (!key.isValid()) {
                    selectorThread.cleanupSelectionKey(key);
                    return;
                }
                // 更新NIOServerCnxn上的时间，NIOServerCnxn也是一个对象，如果没有接收到数据（客户端一直没有发送数据和ping）
                // 服务端就会清除长时间没有活动的客户端，所以这一步是来讲当前客户端延长连接时间的
                touchCnxn(cnxn);
            }

            // Mark this connection as once again ready for selection
            // 请求处理完毕后就可以开启，处理下一个key
            cnxn.enableSelectable();
            // Push an update request on the queue to resume selecting
            // on the current set of interest ops, which may have changed
            // as a result of the I/O operations we just performed.
            if (!selectorThread.addInterestOpsUpdateRequest(key)) {
                cnxn.close();
            }
        }

        @Override
        public void cleanup() {
            cnxn.close();
        }
    }

    /**
     * This thread is responsible for closing stale connections so that
     * connections on which no session is established are properly expired.
     */
    private class ConnectionExpirerThread extends ZooKeeperThread {
        ConnectionExpirerThread() {
            super("ConnnectionExpirer");
        }

        public void run() {
            try {
                while (!stopped) {
                    long waitTime = cnxnExpiryQueue.getWaitTime();
                    if (waitTime > 0) {
                        Thread.sleep(waitTime);
                        continue;
                    }
                    for (NIOServerCnxn conn : cnxnExpiryQueue.poll()) {
                        conn.close();
                    }
                }

            } catch (InterruptedException e) {
                LOG.info("ConnnectionExpirerThread interrupted");
            }
        }
    }

    ServerSocketChannel ss;

    /**
     * We use this buffer to do efficient socket I/O. Because I/O is handled
     * by the worker threads (or the selector threads directly, if no worker
     * thread pool is created), we can create a fixed set of these to be
     * shared by connections.
     */
    private static final ThreadLocal<ByteBuffer> directBuffer =
            new ThreadLocal<ByteBuffer>() {
                @Override
                protected ByteBuffer initialValue() {
                    return ByteBuffer.allocateDirect(directBufferBytes);
                }
            };

    public static ByteBuffer getDirectBuffer() {
        return directBufferBytes > 0 ? directBuffer.get() : null;
    }

    // sessionMap is used by closeSession()
    private final ConcurrentHashMap<Long, NIOServerCnxn> sessionMap =
            new ConcurrentHashMap<Long, NIOServerCnxn>();
    // ipMap is used to limit connections per IP
    private final ConcurrentHashMap<InetAddress, Set<NIOServerCnxn>> ipMap =
            new ConcurrentHashMap<InetAddress, Set<NIOServerCnxn>>();

    protected int maxClientCnxns = 60;

    int sessionlessCnxnTimeout;
    private ExpiryQueue<NIOServerCnxn> cnxnExpiryQueue;


    protected WorkerService workerPool;

    private static int directBufferBytes;
    private int numSelectorThreads;
    private int numWorkerThreads;
    private long workerShutdownTimeoutMS;

    /**
     * Construct a new server connection factory which will accept an unlimited number
     * of concurrent connections from each client (up to the file descriptor
     * limits of the operating system). startup(zks) must be called subsequently.
     */
    public NIOServerCnxnFactory() {
    }

    private volatile boolean stopped = true;
    private ConnectionExpirerThread expirerThread;
    private AcceptThread acceptThread;
    private final Set<SelectorThread> selectorThreads =
            new HashSet<SelectorThread>();

    /**
     * runFromConfig()方法中调用此方法来初始化服务端配置信息，例如SelectorThread最大线程数，sessionlessCnxnTimeout，
     * 开启一个ServerSocketChannel连接，新建一个AcceptThread来接收所有请求
     *
     * @param addr
     * @param maxcc
     * @param secure
     * @throws IOException
     */
    @Override
    public void configure(InetSocketAddress addr, int maxcc, boolean secure) throws IOException {
        if (secure) {
            throw new UnsupportedOperationException("SSL isn't supported in NIOServerCnxn");
        }
        configureSaslLogin();

        maxClientCnxns = maxcc;
        sessionlessCnxnTimeout = Integer.getInteger(
                ZOOKEEPER_NIO_SESSIONLESS_CNXN_TIMEOUT, 10000);
        // We also use the sessionlessCnxnTimeout as expiring interval for
        // cnxnExpiryQueue. These don't need to be the same, but the expiring
        // interval passed into the ExpiryQueue() constructor below should be
        // less than or equal to the timeout.
        cnxnExpiryQueue =
                new ExpiryQueue<NIOServerCnxn>(sessionlessCnxnTimeout);
        expirerThread = new ConnectionExpirerThread();

        int numCores = Runtime.getRuntime().availableProcessors();

        // 根据机器CPU核数计算最大SelectorThread线程数量 -- 32核的线程数为4-- MAX(Math.sqrt(numCores/2), 1)
        numSelectorThreads = Integer.getInteger(
                ZOOKEEPER_NIO_NUM_SELECTOR_THREADS,
                Math.max((int) Math.sqrt((float) numCores / 2), 1));
        if (numSelectorThreads < 1) {
            throw new IOException("numSelectorThreads must be at least 1");
        }

        numWorkerThreads = Integer.getInteger(
                ZOOKEEPER_NIO_NUM_WORKER_THREADS, 2 * numCores);
        workerShutdownTimeoutMS = Long.getLong(
                ZOOKEEPER_NIO_SHUTDOWN_TIMEOUT, 5000);

        LOG.info("Configuring NIO connection handler with "
                + (sessionlessCnxnTimeout / 1000) + "s sessionless connection"
                + " timeout, " + numSelectorThreads + " selector thread(s), "
                + (numWorkerThreads > 0 ? numWorkerThreads : "no")
                + " worker threads, and "
                + (directBufferBytes == 0 ? "gathered writes." :
                ("" + (directBufferBytes / 1024) + " kB direct buffers.")));
        for (int i = 0; i < numSelectorThreads; ++i) {
            selectorThreads.add(new SelectorThread(i));
        }

        // 开启一个ServerSocketChannel
        this.ss = ServerSocketChannel.open();
        ss.socket().setReuseAddress(true);
        LOG.info("binding to port  " + addr);
        // 绑定端口号
        ss.socket().bind(addr);
        // 设置为非阻塞型
        ss.configureBlocking(false);
        // 同时new 一个AcceptThread线程来接收所有的客户端连接请求
        acceptThread = new AcceptThread(ss, addr, selectorThreads);
    }

    private void tryClose(ServerSocketChannel s) {
        try {
            s.close();
        } catch (IOException sse) {
            LOG.error("Error while closing server socket.", sse);
        }
    }

    @Override
    public void reconfigure(InetSocketAddress addr) {
        ServerSocketChannel oldSS = ss;
        try {
            acceptThread.setReconfiguring();
            tryClose(oldSS);
            acceptThread.wakeupSelector();
            try {
                acceptThread.join();
            } catch (InterruptedException e) {
                LOG.error("Error joining old acceptThread when reconfiguring client port {}",
                        e.getMessage());
                Thread.currentThread().interrupt();
            }
            this.ss = ServerSocketChannel.open();
            ss.socket().setReuseAddress(true);
            LOG.info("binding to port " + addr);
            ss.socket().bind(addr);
            ss.configureBlocking(false);
            acceptThread = new AcceptThread(ss, addr, selectorThreads);
            acceptThread.start();
        } catch (IOException e) {
            LOG.error("Error reconfiguring client port to {} {}", addr, e.getMessage());
            tryClose(oldSS);
        }
    }

    /** {@inheritDoc} */
    public int getMaxClientCnxnsPerHost() {
        return maxClientCnxns;
    }

    /** {@inheritDoc} */
    public void setMaxClientCnxnsPerHost(int max) {
        maxClientCnxns = max;
    }

    /**
     * 一个AcceptThread用来接收所有的Socket连接，连接一个Socket就会生成一个SocketChannel，然后交给一个SelectorThread处理，
     */
    @Override
    public void start() {
        stopped = false;
        // 这里对线程池做初始化，用来处理IOWorkRequest
        if (workerPool == null) {
            workerPool = new WorkerService("NIOWorker", numWorkerThreads, false);
        }
        for (SelectorThread thread : selectorThreads) {
            if (thread.getState() == Thread.State.NEW) {
                thread.start();
            }
        }
        // ensure thread is started once and only once
        if (acceptThread.getState() == Thread.State.NEW) {
            acceptThread.start();
        }
        if (expirerThread.getState() == Thread.State.NEW) {
            expirerThread.start();
        }
    }

    /**
     * 新建SelectThreads， AcceptThread线程来接受处理连接请求
     * @param zks
     * @param startServer
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void startup(ZooKeeperServer zks, boolean startServer)
            throws IOException, InterruptedException {
        // 1,新建一个selectorThreads线程池，用来处理连接客户端socket连接
        // 2,启动多个selectorThreads线程，负责接收读写就绪事件
        // 3,启动一个AcceptThread，负责接收连接事件
        start();
        setZooKeeperServer(zks);
        if (startServer) {
            // 1,初始化ZKDatabase，加载数据
            // 2,还会把之前的session给加载出来，放入sessionsWithTimeout中
            zks.startdata();
            // 1,创建sessionTracker
            // 2,初始化requestPrecessChain-请求处理器链-如果某个节点只有一部分权限acdwr，就在RequestPrecessChain里面做的判断
            // 3,创建RequestThrottle
            // 4,注册jmx
            // 5,修改服务端为RUNNING状态
            // 6,NotifyAll，上面步骤中会启动一些线程，这些线程在运行过程中如果发现一写其他的前置条件还没有满足，则去执行前置条件
            zks.startup();
        }
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) ss.socket().getLocalSocketAddress();
    }

    @Override
    public int getLocalPort() {
        return ss.socket().getLocalPort();
    }

    /**
     * De-registers the connection from the various mappings maintained
     * by the factory.
     */
    public boolean removeCnxn(NIOServerCnxn cnxn) {
        // If the connection is not in the master list it's already been closed
        if (!cnxns.remove(cnxn)) {
            return false;
        }
        cnxnExpiryQueue.remove(cnxn);

        long sessionId = cnxn.getSessionId();
        if (sessionId != 0) {
            sessionMap.remove(sessionId);
        }

        InetAddress addr = cnxn.getSocketAddress();
        if (addr != null) {
            Set<NIOServerCnxn> set = ipMap.get(addr);
            if (set != null) {
                set.remove(cnxn);
                // Note that we make no effort here to remove empty mappings
                // from ipMap.
            }
        }

        // unregister from JMX
        unregisterConnection(cnxn);
        return true;
    }

    /**
     * 在CnxnExpiryQueue中添加和更新NIOServerCnxn，防止过期被清理
     * Add or update cnxn in our cnxnExpiryQueue
     * @param cnxn
     */
    public void touchCnxn(NIOServerCnxn cnxn) {
        cnxnExpiryQueue.update(cnxn, cnxn.getSessionTimeout());
    }

    private void addCnxn(NIOServerCnxn cnxn) throws IOException {
        InetAddress addr = cnxn.getSocketAddress();
        if (addr == null) {
            throw new IOException("Socket of " + cnxn + " has been closed");
        }
        Set<NIOServerCnxn> set = ipMap.get(addr);
        if (set == null) {
            // in general we will see 1 connection from each
            // host, setting the initial cap to 2 allows us
            // to minimize mem usage in the common case
            // of 1 entry --  we need to set the initial cap
            // to 2 to avoid rehash when the first entry is added
            // Construct a ConcurrentHashSet using a ConcurrentHashMap
            set = Collections.newSetFromMap(
                    new ConcurrentHashMap<NIOServerCnxn, Boolean>(2));
            // Put the new set in the map, but only if another thread
            // hasn't beaten us to it
            Set<NIOServerCnxn> existingSet = ipMap.putIfAbsent(addr, set);
            if (existingSet != null) {
                set = existingSet;
            }
        }
        set.add(cnxn);

        cnxns.add(cnxn);
        touchCnxn(cnxn);
    }

    protected NIOServerCnxn createConnection(SocketChannel sock,
                                             SelectionKey sk, SelectorThread selectorThread) throws IOException {
        return new NIOServerCnxn(zkServer, sock, sk, this, selectorThread);
    }

    private int getClientCnxnCount(InetAddress cl) {
        Set<NIOServerCnxn> s = ipMap.get(cl);
        if (s == null) return 0;
        return s.size();
    }

    /**
     * clear all the connections in the selector
     *
     */
    @Override
    @SuppressWarnings("unchecked")
    public void closeAll() {
        // clear all the connections on which we are selecting
        for (ServerCnxn cnxn : cnxns) {
            try {
                // This will remove the cnxn from cnxns
                cnxn.close();
            } catch (Exception e) {
                LOG.warn("Ignoring exception closing cnxn sessionid 0x"
                        + Long.toHexString(cnxn.getSessionId()), e);
            }
        }
    }

    public void stop() {
        stopped = true;

        // Stop queuing connection attempts
        try {
            ss.close();
        } catch (IOException e) {
            LOG.warn("Error closing listen socket", e);
        }

        if (acceptThread != null) {
            if (acceptThread.isAlive()) {
                acceptThread.wakeupSelector();
            } else {
                acceptThread.closeSelector();
            }
        }
        if (expirerThread != null) {
            expirerThread.interrupt();
        }
        for (SelectorThread thread : selectorThreads) {
            if (thread.isAlive()) {
                thread.wakeupSelector();
            } else {
                thread.closeSelector();
            }
        }
        if (workerPool != null) {
            workerPool.stop();
        }
    }

    public void shutdown() {
        try {
            // close listen socket and signal selector threads to stop
            stop();

            // wait for selector and worker threads to shutdown
            join();

            // close all open connections
            closeAll();

            if (login != null) {
                login.shutdown();
            }
        } catch (InterruptedException e) {
            LOG.warn("Ignoring interrupted exception during shutdown", e);
        } catch (Exception e) {
            LOG.warn("Ignoring unexpected exception during shutdown", e);
        }

        if (zkServer != null) {
            zkServer.shutdown();
        }
    }

    public void addSession(long sessionId, NIOServerCnxn cnxn) {
        sessionMap.put(sessionId, cnxn);
    }

    @Override
    public boolean closeSession(long sessionId) {
        NIOServerCnxn cnxn = sessionMap.remove(sessionId);
        if (cnxn != null) {
            cnxn.close();
            return true;
        }
        return false;
    }

    @Override
    public void join() throws InterruptedException {
        if (acceptThread != null) {
            acceptThread.join();
        }
        for (SelectorThread thread : selectorThreads) {
            thread.join();
        }
        if (workerPool != null) {
            workerPool.join(workerShutdownTimeoutMS);
        }
    }

    @Override
    public Iterable<ServerCnxn> getConnections() {
        return cnxns;
    }

    public void dumpConnections(PrintWriter pwriter) {
        pwriter.print("Connections ");
        cnxnExpiryQueue.dump(pwriter);
    }

    @Override
    public void resetAllConnectionStats() {
        // No need to synchronize since cnxns is backed by a ConcurrentHashMap
        for (ServerCnxn c : cnxns) {
            c.resetStats();
        }
    }

    @Override
    public Iterable<Map<String, Object>> getAllConnectionInfo(boolean brief) {
        HashSet<Map<String, Object>> info = new HashSet<Map<String, Object>>();
        // No need to synchronize since cnxns is backed by a ConcurrentHashMap
        for (ServerCnxn c : cnxns) {
            info.add(c.getConnectionInfo(brief));
        }
        return info;
    }
}
