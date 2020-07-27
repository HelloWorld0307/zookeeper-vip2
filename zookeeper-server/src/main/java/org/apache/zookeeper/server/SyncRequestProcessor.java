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

import java.io.Flushable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SyncRequestProcessor有两个做用：
 * 1，将请求中的request.txn  持久化；
 * 在SyncReqeustProcessor中，不是每接收到一个Reqeust就直接进行持久化，它的思路是批量持久化。
 * SyncReqeustProcessor会不断的接收到读写请求，当接收到先请求时，会把请求写到文件的OutputStream中，并把请求添加到toFlush队列中，
 * 然后就去处理下一个请求，如果下一个请求是一个读请求，则会直接把读请求交给nextProcessor进行处理，如果下一个请求是一个写请求，
 * 和上一个写请求的处理逻辑是一样的，先把请求写到文件的OutputStream中，并把请求添加到toFlush队列中。
 * 如果处理完一个写请求后，一小段时间内没有接收到新的请求，则会把toFlush队列中的请求进行flush（真正把数据持久化到file中去），
 * 并把请求交给nextProcessor进行处理如果处理完一个写请求后，正好满足了应该flush的条件（时间条件和累积的请求个数）则也会进行flush。
 * 对应到java代码：1），new File("log.xxx");
 *               2), 将对象写入到file的OutPutStream的对象中，file.outPutStream.write(request.txn)
 *               3), flush到log.xxx文件中
 *
 * 2，将DataTree直接也就是数据库打一个快照到本地磁盘中
 * DataTree的快照时间不是固定的，具有一定的随机性。打快照的第一个前提的条件是，
 * SyncReqeustProcessor成功的把某个写请求写到了文件的OutputStream中，然后再判断当前还没有flush
 * 的请求的个数和字节总和，如果其中一个条件符合了就会进行打快照。再打快照之前先把之前还没有flush的请
 * 求也flush，然后再单开一个线程去对DataTree进行持久化
 *
 *
 * This RequestProcessor logs requests to disk. It batches the requests to do
 * the io efficiently. The request is not passed to the next RequestProcessor
 * until its log has been synced to disk.
 *
 * SyncRequestProcessor is used in 3 different cases
 * 1. Leader - Sync request to disk and forward it to AckRequestProcessor which
 *             send ack back to itself.
 * 2. Follower - Sync request to disk and forward request to
 *             SendAckRequestProcessor which send the packets to leader.
 *             SendAckRequestProcessor is flushable which allow us to force
 *             push packets to leader.
 * 3. Observer - Sync committed request to disk (received as INFORM packet).
 *             It never send ack back to the leader, so the nextProcessor will
 *             be null. This change the semantic of txnlog on the observer
 *             since it only contains committed txns.
 */
public class SyncRequestProcessor extends ZooKeeperCriticalThread implements
        RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessor.class);
    private final ZooKeeperServer zks;
    private final LinkedBlockingQueue<Request> queuedRequests =
        new LinkedBlockingQueue<Request>();
    private final RequestProcessor nextProcessor;

    private Thread snapInProcess = null;
    volatile private boolean running;

    /**
     * Transactions that have been written and are waiting to be flushed to
     * disk. Basically this is the list of SyncItems whose callbacks will be
     * invoked after flush returns successfully.
     */
    private final LinkedList<Request> toFlush = new LinkedList<Request>();
    private final Random r = new Random();
    /**
     * The number of log entries to log before starting a snapshot
     */
    private static int snapCount = ZooKeeperServer.getSnapCount();

    private final Request requestOfDeath = Request.requestOfDeath;

    public SyncRequestProcessor(ZooKeeperServer zks,
            RequestProcessor nextProcessor) {
        super("SyncThread:" + zks.getServerId(), zks
                .getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        running = true;
    }

    /**
     * used by tests to check for changing
     * snapcounts
     * @param count
     */
    public static void setSnapCount(int count) {
        snapCount = count;
    }

    /**
     * used by tests to get the snapcount
     * @return the snapcount
     */
    public static int getSnapCount() {
        return snapCount;
    }

    // flush 条件：
    // 1,当有读请求过来了会先将toFlush队列中数据写入磁盘，保证读写顺序一致
    // 2,toFlush请求数大于1000
    // 3,当前toFlush队列里有数据，但是已经超过了特定时间段没有数据写入，执行flush方法
    @Override
    public void run() {
        try {
            int logCount = 0;

            // we do this in an attempt to ensure that not all of the servers
            // in the ensemble take a snapshot at the same time
            int randRoll = r.nextInt(snapCount/2);
            while (true) {
                Request si = null;
                if (toFlush.isEmpty()) {
                    si = queuedRequests.take();
                } else {
                    si = queuedRequests.poll();
                    if (si == null) {
                        flush(toFlush);
                        continue;
                    }
                }
                if (si == requestOfDeath) {
                    break;
                }
                // append方法写请求返回true，读请求返回false
                if (si != null) {
                    // si表示Request，此时的Request中已经有了日志头和日志体，可以持久化了
                    // 调用FileTxnLog对象（初始化服务端的时候生成的对象）将请求头hdr和请求体request.txn合并到一起写入到日志文件中
                    if (zks.getZKDatabase().append(si)) {
                        logCount++;
                        if (logCount > (snapCount / 2 + randRoll)) {
                            randRoll = r.nextInt(snapCount/2);
                            // roll the log
                            zks.getZKDatabase().rollLog();
                            // take a snapshot
                            if (snapInProcess != null && snapInProcess.isAlive()) {
                                LOG.warn("Too busy to snap, skipping");
                            } else {
                                snapInProcess = new ZooKeeperThread("Snapshot Thread") {
                                        public void run() {
                                            try {
                                                zks.takeSnapshot();
                                            } catch(Exception e) {
                                                LOG.warn("Unexpected exception", e);
                                            }
                                        }
                                    };
                                snapInProcess.start();
                            }
                            logCount = 0;
                        }
                    } else if (toFlush.isEmpty()) {
                        // 这里优化了大量读请求的工作量，由于读操作不需要记录日志且toFlush是空队列，所以读操作直接跳过进入到下一processor中，
                        // 同时，如果当前操作是读，但是toFlush还有请求，需要执行flush操作，将数据持久化到日志文件中。然后才是接着做读操作
                        if (nextProcessor != null) {
                            // nextProcessor 为FinalRequestProcessor对象，FinalRequestProcessor不是线程，直接调用处理
                            // 1,更新DataTree
                            // 2,触发Watch
                            // 3,返回Response
                            nextProcessor.processRequest(si);
                            // 如果当前是可写入的直接写入log文件中，也可以等到下面步骤写入
                            if (nextProcessor instanceof Flushable) {
                                ((Flushable)nextProcessor).flush();
                            }
                        }
                        continue;
                    }
                    // 把当前请求添加到待flush队列中 ----- 当前日志只是写入到outPutStream中了
                    toFlush.add(si);
                    // 一旦队列中等待flush的请求数大于1000的时候就执行flush方法写入到文件中
                    if (toFlush.size() > 1000) {
                        flush(toFlush);
                    }
                }
            }
        } catch (Throwable t) {
            handleException(this.getName(), t);
        } finally{
            running = false;
        }
        LOG.info("SyncRequestProcessor exited!");
    }

    private void flush(LinkedList<Request> toFlush)
        throws IOException, RequestProcessorException
    {
        if (toFlush.isEmpty())
            return;

        zks.getZKDatabase().commit();
        while (!toFlush.isEmpty()) {
            Request i = toFlush.remove();
            if (nextProcessor != null) {
                nextProcessor.processRequest(i);
            }
        }
        if (nextProcessor != null && nextProcessor instanceof Flushable) {
            ((Flushable)nextProcessor).flush();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        queuedRequests.add(requestOfDeath);
        try {
            if(running){
                this.join();
            }
            if (!toFlush.isEmpty()) {
                flush(toFlush);
            }
        } catch(InterruptedException e) {
            LOG.warn("Interrupted while wating for " + this + " to finish");
        } catch (IOException e) {
            LOG.warn("Got IO exception during shutdown");
        } catch (RequestProcessorException e) {
            LOG.warn("Got request processor exception during shutdown");
        }
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

    public void processRequest(Request request) {
        // request.addRQRec(">sync");
        queuedRequests.add(request);
    }

}
