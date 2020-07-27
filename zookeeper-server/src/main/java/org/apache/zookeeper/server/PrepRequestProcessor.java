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

import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.BadArgumentsException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.MultiTransactionRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.common.StringUtils;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateTTLRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.ReconfigRequest;
import org.apache.zookeeper.proto.SetACLRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.server.ZooKeeperServer.ChangeRecord;
import org.apache.zookeeper.server.auth.AuthenticationProvider;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.server.quorum.Leader.XidRolloverException;
import org.apache.zookeeper.server.quorum.LeaderZooKeeperServer;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.txn.CheckVersionTxn;
import org.apache.zookeeper.txn.CreateContainerTxn;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.CreateTTLTxn;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.DeleteTxn;
import org.apache.zookeeper.txn.ErrorTxn;
import org.apache.zookeeper.txn.MultiTxn;
import org.apache.zookeeper.txn.SetACLTxn;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.Txn;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This request processor is generally at the start of a RequestProcessor
 * change. It sets up any transactions associated with requests that change the
 * state of the system. It counts on ZooKeeperServer to update
 * outstandingRequests, so that it can take into account transactions that are
 * in the queue to be applied when generating a transaction.
 */
public class PrepRequestProcessor extends ZooKeeperCriticalThread implements
        RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(PrepRequestProcessor.class);

    static boolean skipACL;
    static {
        skipACL = System.getProperty("zookeeper.skipACL", "no").equals("yes");
        if (skipACL) {
            LOG.info("zookeeper.skipACL==\"yes\", ACL checks will be skipped");
        }
    }

    /**
     * this is only for testing purposes.
     * should never be used otherwise
     */
    private static  boolean failCreate = false;

    LinkedBlockingQueue<Request> submittedRequests = new LinkedBlockingQueue<Request>();

    private final RequestProcessor nextProcessor;

    ZooKeeperServer zks;

    public PrepRequestProcessor(ZooKeeperServer zks,
            RequestProcessor nextProcessor) {
        super("ProcessThread(sid:" + zks.getServerId() + " cport:"
                + zks.getClientPort() + "):", zks.getZooKeeperServerListener());
        this.nextProcessor = nextProcessor;
        this.zks = zks;
    }

    /**
     * PrepRequestProcessor：
     * 通常是一个Requestprocessor Chain中的第一个Processor，用来预处理请求。
     * 主要包括：
     * 1. 检查ACL，如果不匹配ACL，则直接结束对该请求的处理
     * 2. 生成并记录ChangeRecord
     * 3. 设置持久化txn
     * 4. 调用下一个RequestProcessor
     *
     * 通俗一点理解就是，过滤Request，不是所有的Request都是合法的，所以需要对Request进行合法的验证，验证通过后，
     * 对于Request而言就要进行持久化了，所以PrepRequestProcessor中也为持久化做一下准备，比如生成和Txn和TxnHeader，
     * 在持久化时直接从Request中获取这两个属性进行持久化就行了。
     * 另外，Request持久化完成后，就需要更新DataTree了，并且是根据Txn来更新DataTree（根据持久化的信息来更新DataTree）。
     * 那么，为什么需要ChangeRecord呢？
     * ChangeRecord表示修改记录，表示某个节点的修改记录，在处理Request时，需要依赖现有节点上的已有信息，
     * 比如cversion（某个节点的孩子节点版本），比如，在处理一个create请求时，需要修改父节点上的cversion（加1），
     * 那么这个信息从哪来呢？一开始肯定是从DataTree上来，但是不能每次都从DataTree上来获取父节点的信息，这样性能很慢，
     * 比如ZooKeeperServer连续收到两个create请求，当某个create请求在被处理时，都需要先从DataTree获取信息，然后持久化，
     * 然后更新DataTree，最后才能处理下一个create请求，是一个串行的过程，那么如果第二个create不合法呢？依照上面的思路，
     * 则还需要等待第一个create请求处理完了之后才能对第二个请求进行验证，所以Zookeeper为了解决这个问题，
     * 在PrepRequestProcessor中，没验证完一个请求，就把这个请求异步的交给持久化线程来处理，PrepRequestProcessor自己
     * 就去处理下一个请求了，打断了串行的链路，但是这时又出现了问题，因为在处理第二个create请求时需要依赖父节点的信息，
     * 并且应该处理过第一个create请求后的结果，所以这时就引入了ChangeRecord，PrepRequestProcessor在处理第一个create请求时，
     * 先生成一条ChangeRecord记录，然后再异步的去持久化和更新DataTree，然后立即去处理第二个create请求，
     * 此时就可以不需要去取DataTree中的信息了（就算取了，可能取到的信息也不对），就直接取ChangeRecord中的信息就可以了。
     *
     * 问题一：cversion是什么时候去ChangeRecord中拿，什么是从DataTree中拿？
     * 答：先去ChangeRecord中拿，如果没拿到说明之前对这个节点没有处理过，拿到了直接只用拿到的信息。
     * @param b
     */
    public static void setFailCreate(boolean b) {
        failCreate = b;
    }
    @Override
    public void run() {
        try {
            while (true) {
                // PrepRequestProcessor 从队列中拿到一个Request请求对象
                Request request = submittedRequests.take();
                // 下面是日志打印逻辑
                long traceMask = ZooTrace.CLIENT_REQUEST_TRACE_MASK;
                if (request.type == OpCode.ping) {
                    traceMask = ZooTrace.CLIENT_PING_TRACE_MASK;
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logRequest(LOG, traceMask, 'P', request, "");
                }
                // 这里是对一个线程shutdown的优雅方法，如果线程为某种状态后直接break掉即可。
                if (Request.requestOfDeath == request) {
                    break;
                }
                // 处理请求
                pRequest(request);
            }
        } catch (RequestProcessorException e) {
            if (e.getCause() instanceof XidRolloverException) {
                LOG.info(e.getCause().getMessage());
            }
            handleException(this.getName(), e);
        } catch (Exception e) {
            handleException(this.getName(), e);
        }
        LOG.info("PrepRequestProcessor exited loop!");
    }

    /**
     * 此方法可以用来解决多线程环境下cversion不一致问题
     *
     * @param path
     * @return
     * @throws KeeperException.NoNodeException
     */
    private ChangeRecord getRecordForPath(String path) throws KeeperException.NoNodeException {
        ChangeRecord lastChange = null;
        synchronized (zks.outstandingChanges) {
            // 从一个Map<String, ChangeRecord>类型的集合中找是否有当前节点信息，有说明已经之前修改过了，直接使用；没有去DataTree中拿
            lastChange = zks.outstandingChangesForPath.get(path);
            if (lastChange == null) {
                DataNode n = zks.getZKDatabase().getNode(path);
                if (n != null) {
                    Set<String> children;
                    synchronized(n) {
                        children = n.getChildren();
                    }
                    lastChange = new ChangeRecord(-1, path, n.stat, children.size(),
                            zks.getZKDatabase().aclForNode(n));
                }
            }
        }
        if (lastChange == null || lastChange.stat == null) {
            throw new KeeperException.NoNodeException(path);
        }
        return lastChange;
    }

    private ChangeRecord getOutstandingChange(String path) {
        synchronized (zks.outstandingChanges) {
            return zks.outstandingChangesForPath.get(path);
        }
    }

    private void addChangeRecord(ChangeRecord c) {
        synchronized (zks.outstandingChanges) {
            zks.outstandingChanges.add(c);
            zks.outstandingChangesForPath.put(c.path, c);
        }
    }

    /**
     * Grab current pending change records for each op in a multi-op.
     *
     * This is used inside MultiOp error code path to rollback in the event
     * of a failed multi-op.
     *
     * @param multiRequest
     * @return a map that contains previously existed records that probably need to be
     *         rolled back in any failure.
     */
    private Map<String, ChangeRecord> getPendingChanges(MultiTransactionRecord multiRequest) {
        HashMap<String, ChangeRecord> pendingChangeRecords = new HashMap<String, ChangeRecord>();

        for (Op op : multiRequest) {
            String path = op.getPath();
            ChangeRecord cr = getOutstandingChange(path);
            // only previously existing records need to be rolled back.
            if (cr != null) {
                pendingChangeRecords.put(path, cr);
            }

            /*
             * ZOOKEEPER-1624 - We need to store for parent's ChangeRecord
             * of the parent node of a request. So that if this is a
             * sequential node creation request, rollbackPendingChanges()
             * can restore previous parent's ChangeRecord correctly.
             *
             * Otherwise, sequential node name generation will be incorrect
             * for a subsequent request.
             */
            int lastSlash = path.lastIndexOf('/');
            if (lastSlash == -1 || path.indexOf('\0') != -1) {
                continue;
            }
            String parentPath = path.substring(0, lastSlash);
            ChangeRecord parentCr = getOutstandingChange(parentPath);
            if (parentCr != null) {
                pendingChangeRecords.put(parentPath, parentCr);
            }
        }

        return pendingChangeRecords;
    }

    /**
     * Rollback pending changes records from a failed multi-op.
     *
     * If a multi-op fails, we can't leave any invalid change records we created
     * around. We also need to restore their prior value (if any) if their prior
     * value is still valid.
     *
     * @param zxid
     * @param pendingChangeRecords
     */
    void rollbackPendingChanges(long zxid, Map<String, ChangeRecord>pendingChangeRecords) {
        synchronized (zks.outstandingChanges) {
            // Grab a list iterator starting at the END of the list so we can iterate in reverse
            Iterator<ChangeRecord> iter = zks.outstandingChanges.descendingIterator();
            while (iter.hasNext()) {
                ChangeRecord c = iter.next();
                if (c.zxid == zxid) {
                    iter.remove();
                    // Remove all outstanding changes for paths of this multi.
                    // Previous records will be added back later.
                    zks.outstandingChangesForPath.remove(c.path);
                } else {
                    break;
                }
            }

            // we don't need to roll back any records because there is nothing left.
            if (zks.outstandingChanges.isEmpty()) {
                return;
            }

            long firstZxid = zks.outstandingChanges.peek().zxid;

            for (ChangeRecord c : pendingChangeRecords.values()) {
                // Don't apply any prior change records less than firstZxid.
                // Note that previous outstanding requests might have been removed
                // once they are completed.
                if (c.zxid < firstZxid) {
                    continue;
                }

                // add previously existing records back.
                zks.outstandingChangesForPath.put(c.path, c);
            }
        }
    }

    /**
     * Grant or deny authorization to an operation on a node as a function of:
     *
     * @param zks: not used.
     * @param acl:  set of ACLs for the node
     * @param perm: the permission that the client is requesting
     * @param ids:  the credentials supplied by the client
     */
    static void checkACL(ZooKeeperServer zks, List<ACL> acl, int perm,
            List<Id> ids) throws KeeperException.NoAuthException {
        if (skipACL) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Permission requested: {} ", perm);
            LOG.debug("ACLs for node: {}", acl);
            LOG.debug("Client credentials: {}", ids);
        }
        if (acl == null || acl.size() == 0) {
            return;
        }
        for (Id authId : ids) {
            if (authId.getScheme().equals("super")) {
                return;
            }
        }
        for (ACL a : acl) {
            Id id = a.getId();
            if ((a.getPerms() & perm) != 0) {
                if (id.getScheme().equals("world")
                        && id.getId().equals("anyone")) {
                    return;
                }
                AuthenticationProvider ap = ProviderRegistry.getProvider(id
                        .getScheme());
                if (ap != null) {
                    for (Id authId : ids) {
                        if (authId.getScheme().equals(id.getScheme())
                                && ap.matches(authId.getId(), id.getId())) {
                            return;
                        }
                    }
                }
            }
        }
        throw new KeeperException.NoAuthException();
    }

    /**
     * Performs basic validation of a path for a create request.
     * Throws if the path is not valid and returns the parent path.
     * @throws BadArgumentsException
     */
    private String validatePathForCreate(String path, long sessionId)
            throws BadArgumentsException {
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash == -1 || path.indexOf('\0') != -1 || failCreate) {
            LOG.info("Invalid path %s with session 0x%s",
                    path, Long.toHexString(sessionId));
            throw new KeeperException.BadArgumentsException(path);
        }
        return path.substring(0, lastSlash);
    }

    /**
     * 持久化当前操作
     * This method will be called inside the ProcessRequestThread, which is a
     * singleton, so there will be a single thread calling this code.
     *
     * @param type
     * @param zxid
     * @param request
     * @param record
     */
    protected void pRequest2Txn(int type, long zxid, Request request,
                                Record record, boolean deserialize)
        throws KeeperException, IOException, RequestProcessorException
    {
        // 设置日志头，具体参数：sessionID，cxid，zxid，当前时间，请求类型
        request.setHdr(new TxnHeader(request.sessionId, request.cxid, zxid,
                Time.currentWallTime(), type));

        switch (type) {
            case OpCode.create:
            case OpCode.create2:
            case OpCode.createTTL:
            case OpCode.createContainer: {
                // record为日志体
                pRequest2TxnCreate(type, request, record, deserialize);
                break;
            }
            case OpCode.deleteContainer: {
                String path = new String(request.request.array());
                String parentPath = getParentPathAndValidate(path);
                ChangeRecord parentRecord = getRecordForPath(parentPath);
                ChangeRecord nodeRecord = getRecordForPath(path);
                if (nodeRecord.childCount > 0) {
                    throw new KeeperException.NotEmptyException(path);
                }
                if (EphemeralType.get(nodeRecord.stat.getEphemeralOwner()) == EphemeralType.NORMAL) {
                    throw new KeeperException.BadVersionException(path);
                }
                request.setTxn(new DeleteTxn(path));
                parentRecord = parentRecord.duplicate(request.getHdr().getZxid());
                parentRecord.childCount--;
                addChangeRecord(parentRecord);
                addChangeRecord(new ChangeRecord(request.getHdr().getZxid(), path, null, -1, null));
                break;
            }
            case OpCode.delete:
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                DeleteRequest deleteRequest = (DeleteRequest)record;
                if(deserialize)
                    ByteBufferInputStream.byteBuffer2Record(request.request, deleteRequest);
                String path = deleteRequest.getPath();
                String parentPath = getParentPathAndValidate(path);
                ChangeRecord parentRecord = getRecordForPath(parentPath);
                ChangeRecord nodeRecord = getRecordForPath(path);
                checkACL(zks, parentRecord.acl, ZooDefs.Perms.DELETE, request.authInfo);
                checkAndIncVersion(nodeRecord.stat.getVersion(), deleteRequest.getVersion(), path);
                if (nodeRecord.childCount > 0) {
                    throw new KeeperException.NotEmptyException(path);
                }
                request.setTxn(new DeleteTxn(path));
                parentRecord = parentRecord.duplicate(request.getHdr().getZxid());
                parentRecord.childCount--;
                addChangeRecord(parentRecord);
                addChangeRecord(new ChangeRecord(request.getHdr().getZxid(), path, null, -1, null));
                break;
            case OpCode.setData:
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                SetDataRequest setDataRequest = (SetDataRequest)record;
                if(deserialize)
                    ByteBufferInputStream.byteBuffer2Record(request.request, setDataRequest);
                path = setDataRequest.getPath();
                validatePath(path, request.sessionId);
                nodeRecord = getRecordForPath(path);
                checkACL(zks, nodeRecord.acl, ZooDefs.Perms.WRITE, request.authInfo);
                int newVersion = checkAndIncVersion(nodeRecord.stat.getVersion(), setDataRequest.getVersion(), path);
                request.setTxn(new SetDataTxn(path, setDataRequest.getData(), newVersion));
                nodeRecord = nodeRecord.duplicate(request.getHdr().getZxid());
                nodeRecord.stat.setVersion(newVersion);
                addChangeRecord(nodeRecord);
                break;
            case OpCode.reconfig:
                if (!QuorumPeerConfig.isReconfigEnabled()) {
                    LOG.error("Reconfig operation requested but reconfig feature is disabled.");
                    throw new KeeperException.ReconfigDisabledException();
                }

                if (skipACL) {
                    LOG.warn("skipACL is set, reconfig operation will skip ACL checks!");
                }

                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                ReconfigRequest reconfigRequest = (ReconfigRequest)record; 
                LeaderZooKeeperServer lzks;
                try {
                    lzks = (LeaderZooKeeperServer)zks;
                } catch (ClassCastException e) {
                    // standalone mode - reconfiguration currently not supported
                    throw new KeeperException.UnimplementedException();
                }
                QuorumVerifier lastSeenQV = lzks.self.getLastSeenQuorumVerifier();                                                                                 
                // check that there's no reconfig in progress
                if (lastSeenQV.getVersion()!=lzks.self.getQuorumVerifier().getVersion()) {
                       throw new KeeperException.ReconfigInProgress(); 
                }
                long configId = reconfigRequest.getCurConfigId();
  
                if (configId != -1 && configId!=lzks.self.getLastSeenQuorumVerifier().getVersion()){
                   String msg = "Reconfiguration from version " + configId + " failed -- last seen version is " +
                           lzks.self.getLastSeenQuorumVerifier().getVersion();
                   throw new KeeperException.BadVersionException(msg);
                }

                String newMembers = reconfigRequest.getNewMembers();
                
                if (newMembers != null) { //non-incremental membership change                  
                   LOG.info("Non-incremental reconfig");
                
                   // Input may be delimited by either commas or newlines so convert to common newline separated format
                   newMembers = newMembers.replaceAll(",", "\n");
                   
                   try{
                       Properties props = new Properties();                        
                       props.load(new StringReader(newMembers));
                       request.qv = QuorumPeerConfig.parseDynamicConfig(props, lzks.self.getElectionType(), true, false);
                       request.qv.setVersion(request.getHdr().getZxid());
                   } catch (IOException | ConfigException e) {
                       throw new KeeperException.BadArgumentsException(e.getMessage());
                   }
                } else { //incremental change - must be a majority quorum system   
                   LOG.info("Incremental reconfig");
                   
                   List<String> joiningServers = null; 
                   String joiningServersString = reconfigRequest.getJoiningServers();
                   if (joiningServersString != null)
                   {
                       joiningServers = StringUtils.split(joiningServersString,",");
                   }
                   
                   List<String> leavingServers = null;
                   String leavingServersString = reconfigRequest.getLeavingServers();
                   if (leavingServersString != null)
                   {
                       leavingServers = StringUtils.split(leavingServersString, ",");
                   }
                   
                   if (!(lastSeenQV instanceof QuorumMaj)) {
                           String msg = "Incremental reconfiguration requested but last configuration seen has a non-majority quorum system";
                           LOG.warn(msg);
                           throw new KeeperException.BadArgumentsException(msg);               
                   }
                   Map<Long, QuorumServer> nextServers = new HashMap<Long, QuorumServer>(lastSeenQV.getAllMembers());
                   try {                           
                       if (leavingServers != null) {
                           for (String leaving: leavingServers){
                               long sid = Long.parseLong(leaving);
                               nextServers.remove(sid);
                           } 
                       }
                       if (joiningServers != null) {
                           for (String joiner: joiningServers){
                        	   // joiner should have the following format: server.x = server_spec;client_spec               
                        	   String[] parts = StringUtils.split(joiner, "=").toArray(new String[0]);
                               if (parts.length != 2) {
                                   throw new KeeperException.BadArgumentsException("Wrong format of server string");
                               }
                               // extract server id x from first part of joiner: server.x
                               Long sid = Long.parseLong(parts[0].substring(parts[0].lastIndexOf('.') + 1));
                               QuorumServer qs = new QuorumServer(sid, parts[1]);
                               if (qs.clientAddr == null || qs.electionAddr == null || qs.addr == null) {
                                   throw new KeeperException.BadArgumentsException("Wrong format of server string - each server should have 3 ports specified"); 	   
                               }

                               // check duplication of addresses and ports
                               for (QuorumServer nqs: nextServers.values()) {
                                   if (qs.id == nqs.id) {
                                       continue;
                                   }
                                   qs.checkAddressDuplicate(nqs);
                               }

                               nextServers.remove(qs.id);
                               nextServers.put(qs.id, qs);
                           }  
                       }
                   } catch (ConfigException e){
                       throw new KeeperException.BadArgumentsException("Reconfiguration failed");
                   }
                   request.qv = new QuorumMaj(nextServers);
                   request.qv.setVersion(request.getHdr().getZxid());
                }
                if (QuorumPeerConfig.isStandaloneEnabled() && request.qv.getVotingMembers().size() < 2) {
                   String msg = "Reconfig failed - new configuration must include at least 2 followers";
                   LOG.warn(msg);
                   throw new KeeperException.BadArgumentsException(msg);
                } else if (request.qv.getVotingMembers().size() < 1) {
                   String msg = "Reconfig failed - new configuration must include at least 1 follower";
                   LOG.warn(msg);
                   throw new KeeperException.BadArgumentsException(msg);
                }                           
                   
                if (!lzks.getLeader().isQuorumSynced(request.qv)) {
                   String msg2 = "Reconfig failed - there must be a connected and synced quorum in new configuration";
                   LOG.warn(msg2);             
                   throw new KeeperException.NewConfigNoQuorum();
                }
                
                nodeRecord = getRecordForPath(ZooDefs.CONFIG_NODE);               
                checkACL(zks, nodeRecord.acl, ZooDefs.Perms.WRITE, request.authInfo);                  
                request.setTxn(new SetDataTxn(ZooDefs.CONFIG_NODE, request.qv.toString().getBytes(), -1));    
                nodeRecord = nodeRecord.duplicate(request.getHdr().getZxid());
                nodeRecord.stat.setVersion(-1);                
                addChangeRecord(nodeRecord);
                break;                         
            case OpCode.setACL:
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                SetACLRequest setAclRequest = (SetACLRequest)record;
                if(deserialize)
                    ByteBufferInputStream.byteBuffer2Record(request.request, setAclRequest);
                path = setAclRequest.getPath();
                validatePath(path, request.sessionId);
                List<ACL> listACL = fixupACL(path, request.authInfo, setAclRequest.getAcl());
                nodeRecord = getRecordForPath(path);
                checkACL(zks, nodeRecord.acl, ZooDefs.Perms.ADMIN, request.authInfo);
                newVersion = checkAndIncVersion(nodeRecord.stat.getAversion(), setAclRequest.getVersion(), path);
                request.setTxn(new SetACLTxn(path, listACL, newVersion));
                nodeRecord = nodeRecord.duplicate(request.getHdr().getZxid());
                nodeRecord.stat.setAversion(newVersion);
                addChangeRecord(nodeRecord);
                break;
            case OpCode.createSession:
                request.request.rewind();
                int to = request.request.getInt();
                request.setTxn(new CreateSessionTxn(to));
                request.request.rewind();
                if (request.isLocalSession()) {
                    // This will add to local session tracker if it is enabled
                    zks.sessionTracker.addSession(request.sessionId, to);
                } else {
                    // Explicitly add to global session if the flag is not set
                    zks.sessionTracker.addGlobalSession(request.sessionId, to);
                }
                zks.setOwner(request.sessionId, request.getOwner());
                break;
            case OpCode.closeSession:
                // We don't want to do this check since the session expiration thread
                // queues up this operation without being the session owner.
                // this request is the last of the session so it should be ok
                //zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                Set<String> es = zks.getZKDatabase()
                        .getEphemerals(request.sessionId);
                synchronized (zks.outstandingChanges) {
                    for (ChangeRecord c : zks.outstandingChanges) {
                        if (c.stat == null) {
                            // Doing a delete
                            es.remove(c.path);
                        } else if (c.stat.getEphemeralOwner() == request.sessionId) {
                            es.add(c.path);
                        }
                    }
                    for (String path2Delete : es) {
                        addChangeRecord(new ChangeRecord(request.getHdr().getZxid(), path2Delete, null, 0, null));
                    }

                    zks.sessionTracker.setSessionClosing(request.sessionId);
                }
                break;
            case OpCode.check:
                zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                CheckVersionRequest checkVersionRequest = (CheckVersionRequest)record;
                if(deserialize)
                    ByteBufferInputStream.byteBuffer2Record(request.request, checkVersionRequest);
                path = checkVersionRequest.getPath();
                validatePath(path, request.sessionId);
                nodeRecord = getRecordForPath(path);
                checkACL(zks, nodeRecord.acl, ZooDefs.Perms.READ, request.authInfo);
                request.setTxn(new CheckVersionTxn(path, checkAndIncVersion(nodeRecord.stat.getVersion(),
                        checkVersionRequest.getVersion(), path)));
                break;
            default:
                LOG.warn("unknown type " + type);
                break;
        }
    }

    private void pRequest2TxnCreate(int type, Request request, Record record, boolean deserialize) throws IOException, KeeperException {
        if (deserialize) {
            // request.request为一个ByteBuffer类型的
            // 把Request请求对象中的内容反序列化到record中，后面会把record持久化
            ByteBufferInputStream.byteBuffer2Record(request.request, record);
        }

        int flags;
        String path;
        List<ACL> acl;
        byte[] data;
        long ttl;
        if (type == OpCode.createTTL) {
            CreateTTLRequest createTtlRequest = (CreateTTLRequest)record;
            flags = createTtlRequest.getFlags();
            path = createTtlRequest.getPath();
            acl = createTtlRequest.getAcl();
            data = createTtlRequest.getData();
            ttl = createTtlRequest.getTtl();
        } else {
            CreateRequest createRequest = (CreateRequest)record;
            flags = createRequest.getFlags();
            path = createRequest.getPath();
            acl = createRequest.getAcl();
            data = createRequest.getData();
            ttl = -1;
        }
        // 七种节点类型CreateMode.PERSISTENT;
        //        CreateMode.EPHEMERAL;
        //        CreateMode.PERSISTENT_SEQUENTIAL;
        //        CreateMode.EPHEMERAL_SEQUENTIAL ;
        //        CreateMode.CONTAINER;
        //        CreateMode.PERSISTENT_WITH_TTL;
        //        CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL;
        CreateMode createMode = CreateMode.fromFlag(flags);
        // 如果为ttl节点，必须服务端必须开启支持ttl
        validateCreateRequest(path, createMode, request, ttl);
        // 当前节点的父节点
        String parentPath = validatePathForCreate(path, request.sessionId);

        List<ACL> listACL = fixupACL(path, request.authInfo, acl);
        // 拿到父节点的ChangeRecord，现在是创建一个节点，所以父节点上的属性也需要修改
        ChangeRecord parentRecord = getRecordForPath(parentPath);

        checkACL(zks, parentRecord.acl, ZooDefs.Perms.CREATE, request.authInfo);
        int parentCVersion = parentRecord.stat.getCversion();
        // 如果创建的是顺序节点，需要给节点拼接一个递增的10位数字后缀
        if (createMode.isSequential()) {
            path = path + String.format(Locale.ENGLISH, "%010d", parentCVersion);
        }
        validatePath(path, request.sessionId);
        try {
            if (getRecordForPath(path) != null) {
                throw new KeeperException.NodeExistsException(path);
            }
        } catch (KeeperException.NoNodeException e) {
            // ignore this one
        }
        // 如果父节点为临时节点，则不能添加子节点，然后直接返回异常信息
        boolean ephemeralParent = EphemeralType.get(parentRecord.stat.getEphemeralOwner()) == EphemeralType.NORMAL;
        if (ephemeralParent) {
            throw new KeeperException.NoChildrenForEphemeralsException(path);
        }
        // 父节点新增一个节点后其cversion要加一
        int newCversion = parentRecord.stat.getCversion()+1;
        // 根据不同节点类型记录日志
        if (type == OpCode.createContainer) { // 容器类型
            request.setTxn(new CreateContainerTxn(path, data, listACL, newCversion));
        } else if (type == OpCode.createTTL) { // ttl类型
            request.setTxn(new CreateTTLTxn(path, data, listACL, newCversion, ttl));
        } else {
            request.setTxn(new CreateTxn(path, data, listACL, createMode.isEphemeral(),
                    newCversion));
        }
        StatPersisted s = new StatPersisted();
        if (createMode.isEphemeral()) {
            s.setEphemeralOwner(request.sessionId);
        }
        parentRecord = parentRecord.duplicate(request.getHdr().getZxid());
        parentRecord.childCount++;
        parentRecord.stat.setCversion(newCversion);
        addChangeRecord(parentRecord);
        addChangeRecord(new ChangeRecord(request.getHdr().getZxid(), path, s, 0, listACL));
    }

    private void validatePath(String path, long sessionId) throws BadArgumentsException {
        try {
            PathUtils.validatePath(path);
        } catch(IllegalArgumentException ie) {
            LOG.info("Invalid path {} with session 0x{}, reason: {}",
                    path, Long.toHexString(sessionId), ie.getMessage());
            throw new BadArgumentsException(path);
        }
    }

    private String getParentPathAndValidate(String path)
            throws BadArgumentsException {
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash == -1 || path.indexOf('\0') != -1
                || zks.getZKDatabase().isSpecialPath(path)) {
            throw new BadArgumentsException(path);
        }
        return path.substring(0, lastSlash);
    }

    private static int checkAndIncVersion(int currentVersion, int expectedVersion, String path)
            throws KeeperException.BadVersionException {
        if (expectedVersion != -1 && expectedVersion != currentVersion) {
            throw new KeeperException.BadVersionException(path);
        }
        return currentVersion + 1;
    }

    /**
     * This method will be called inside the ProcessRequestThread, which is a
     * singleton, so there will be a single thread calling this code.
     *
     * @param request
     */
    protected void pRequest(Request request) throws RequestProcessorException {
        // LOG.info("Prep>>> cxid = " + request.cxid + " type = " +
        // request.type + " id = 0x" + Long.toHexString(request.sessionId));
        //清空 日志头和日志内容，下面会重新赋值
        request.setHdr(null); // 日志头
        request.setTxn(null); // 日志体

        try {
            // 判断不同操作类型
            switch (request.type) {
                // 只有数据操作命令才会调用pRequest2Txn方法记录日志，读命令不需要
            case OpCode.createContainer:
            case OpCode.create:
            case OpCode.create2:
                // 这里new出来的这个对象，只在pRequest2Txn方法中用了，该对象是Record的实现来，表示日志体
                CreateRequest create2Request = new CreateRequest();
                // 记录日志
                // 注意这里先调用zks.getNextZxid() 获取下一个zxid（自增）
                // request对象是Record的子类，该方法内将request赋值给，zxid是一个自增的，为了保证唯一性，每次zookeeper服务器启动的时候都会去
                // 持久化文件中找到最大的zxid作为起始值
                // create2Request 一个空的日志体
                pRequest2Txn(request.type, zks.getNextZxid(), request, create2Request, true);
                break;
            case OpCode.createTTL:
                CreateTTLRequest createTtlRequest = new CreateTTLRequest();
                pRequest2Txn(request.type, zks.getNextZxid(), request, createTtlRequest, true);
                break;
            case OpCode.deleteContainer:
            case OpCode.delete:
                DeleteRequest deleteRequest = new DeleteRequest();
                pRequest2Txn(request.type, zks.getNextZxid(), request, deleteRequest, true);
                break;
            case OpCode.setData:
                SetDataRequest setDataRequest = new SetDataRequest();                
                pRequest2Txn(request.type, zks.getNextZxid(), request, setDataRequest, true);
                break;
            case OpCode.reconfig:
                ReconfigRequest reconfigRequest = new ReconfigRequest();
                ByteBufferInputStream.byteBuffer2Record(request.request, reconfigRequest);
                pRequest2Txn(request.type, zks.getNextZxid(), request, reconfigRequest, true);
                break;
            case OpCode.setACL:
                SetACLRequest setAclRequest = new SetACLRequest();                
                pRequest2Txn(request.type, zks.getNextZxid(), request, setAclRequest, true);
                break;
            case OpCode.check:
                CheckVersionRequest checkRequest = new CheckVersionRequest();              
                pRequest2Txn(request.type, zks.getNextZxid(), request, checkRequest, true);
                break;
            case OpCode.multi:
                MultiTransactionRecord multiRequest = new MultiTransactionRecord();
                try {
                    ByteBufferInputStream.byteBuffer2Record(request.request, multiRequest);
                } catch(IOException e) {
                    request.setHdr(new TxnHeader(request.sessionId, request.cxid, zks.getNextZxid(),
                            Time.currentWallTime(), OpCode.multi));
                    throw e;
                }
                List<Txn> txns = new ArrayList<Txn>();
                //Each op in a multi-op must have the same zxid!
                long zxid = zks.getNextZxid();
                KeeperException ke = null;

                //Store off current pending change records in case we need to rollback
                Map<String, ChangeRecord> pendingChanges = getPendingChanges(multiRequest);

                for(Op op: multiRequest) {
                    Record subrequest = op.toRequestRecord();
                    int type;
                    Record txn;

                    /* If we've already failed one of the ops, don't bother
                     * trying the rest as we know it's going to fail and it
                     * would be confusing in the logfiles.
                     */
                    if (ke != null) {
                        type = OpCode.error;
                        txn = new ErrorTxn(Code.RUNTIMEINCONSISTENCY.intValue());
                    }

                    /* Prep the request and convert to a Txn */
                    else {
                        try {
                            pRequest2Txn(op.getType(), zxid, request, subrequest, false);
                            type = request.getHdr().getType();
                            txn = request.getTxn();
                        } catch (KeeperException e) {
                            ke = e;
                            type = OpCode.error;
                            txn = new ErrorTxn(e.code().intValue());

                            if (e.code().intValue() > Code.APIERROR.intValue()) {
                                LOG.info("Got user-level KeeperException when processing {} aborting" +
                                        " remaining multi ops. Error Path:{} Error:{}",
                                        request.toString(), e.getPath(), e.getMessage());
                            }

                            request.setException(e);

                            /* Rollback change records from failed multi-op */
                            rollbackPendingChanges(zxid, pendingChanges);
                        }
                    }

                    //FIXME: I don't want to have to serialize it here and then
                    //       immediately deserialize in next processor. But I'm
                    //       not sure how else to get the txn stored into our list.
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
                    txn.serialize(boa, "request") ;
                    ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());

                    txns.add(new Txn(type, bb.array()));
                }

                request.setHdr(new TxnHeader(request.sessionId, request.cxid, zxid,
                        Time.currentWallTime(), request.type));
                request.setTxn(new MultiTxn(txns));

                break;

            //create/close session don't require request record
            case OpCode.createSession:
            case OpCode.closeSession:
                if (!request.isLocalSession()) {
                    pRequest2Txn(request.type, zks.getNextZxid(), request,
                                 null, true);
                }
                break;

            //All the rest don't need to create a Txn - just verify session
            case OpCode.sync:
            case OpCode.exists:
            case OpCode.getData:
            case OpCode.getACL:
            case OpCode.getChildren:
            case OpCode.getChildren2:
            case OpCode.ping:
            case OpCode.setWatches:
            case OpCode.checkWatches:
            case OpCode.removeWatches:
                zks.sessionTracker.checkSession(request.sessionId,
                        request.getOwner());
                break;
            default:
                LOG.warn("unknown type " + request.type);
                break;
            }
        } catch (KeeperException e) {
            if (request.getHdr() != null) {
                request.getHdr().setType(OpCode.error);
                request.setTxn(new ErrorTxn(e.code().intValue()));
            }

            if (e.code().intValue() > Code.APIERROR.intValue()) {
                LOG.info("Got user-level KeeperException when processing {} Error Path:{} Error:{}",
                        request.toString(), e.getPath(), e.getMessage());
            }
            request.setException(e);
        } catch (Exception e) {
            // log at error level as we are returning a marshalling
            // error to the user
            LOG.error("Failed to process " + request, e);

            StringBuilder sb = new StringBuilder();
            ByteBuffer bb = request.request;
            if(bb != null){
                bb.rewind();
                while (bb.hasRemaining()) {
                    sb.append(Integer.toHexString(bb.get() & 0xff));
                }
            } else {
                sb.append("request buffer is null");
            }

            LOG.error("Dumping request buffer: 0x" + sb.toString());
            if (request.getHdr() != null) {
                request.getHdr().setType(OpCode.error);
                request.setTxn(new ErrorTxn(Code.MARSHALLINGERROR.intValue()));
            }
        }
        // 生成一个新的id
        request.zxid = zks.getZxid();
        // 这里的nextProcessor其实是在setupRequestProcessors方法初始化中完成的赋值，即为SynchRequestProcessor对象
        // PreRequestProcessor处理完，将request存放到SynchRequestProcessor内部队列中
        nextProcessor.processRequest(request);
    }

    private List<ACL> removeDuplicates(List<ACL> acl) {

        LinkedList<ACL> retval = new LinkedList<ACL>();
        for (ACL a : acl) {
            if (!retval.contains(a)) {
                retval.add(a);
            }
        }
        return retval;
    }

    private void validateCreateRequest(String path, CreateMode createMode, Request request, long ttl)
            throws KeeperException {
        if (createMode.isTTL() && !EphemeralType.extendedEphemeralTypesEnabled()) {
            throw new KeeperException.UnimplementedException();
        }
        try {
            EphemeralType.validateTTL(createMode, ttl);
        } catch (IllegalArgumentException e) {
            throw new BadArgumentsException(path);
        }
        if (createMode.isEphemeral()) {
            // Exception is set when local session failed to upgrade
            // so we just need to report the error
            if (request.getException() != null) {
                throw request.getException();
            }
            zks.sessionTracker.checkGlobalSession(request.sessionId,
                    request.getOwner());
        } else {
            zks.sessionTracker.checkSession(request.sessionId,
                    request.getOwner());
        }
    }

    /**
     * This method checks out the acl making sure it isn't null or empty,
     * it has valid schemes and ids, and expanding any relative ids that
     * depend on the requestor's authentication information.
     *
     * @param authInfo list of ACL IDs associated with the client connection
     * @param acls list of ACLs being assigned to the node (create or setACL operation)
     * @return verified and expanded ACLs
     * @throws KeeperException.InvalidACLException
     */
    private List<ACL> fixupACL(String path, List<Id> authInfo, List<ACL> acls)
        throws KeeperException.InvalidACLException {
        // check for well formed ACLs
        // This resolves https://issues.apache.org/jira/browse/ZOOKEEPER-1877
        List<ACL> uniqacls = removeDuplicates(acls);
        LinkedList<ACL> rv = new LinkedList<ACL>();
        if (uniqacls == null || uniqacls.size() == 0) {
            throw new KeeperException.InvalidACLException(path);
        }
        for (ACL a: uniqacls) {
            LOG.debug("Processing ACL: {}", a);
            if (a == null) {
                throw new KeeperException.InvalidACLException(path);
            }
            Id id = a.getId();
            if (id == null || id.getScheme() == null) {
                throw new KeeperException.InvalidACLException(path);
            }
            if (id.getScheme().equals("world") && id.getId().equals("anyone")) {
                rv.add(a);
            } else if (id.getScheme().equals("auth")) {
                // This is the "auth" id, so we have to expand it to the
                // authenticated ids of the requestor
                boolean authIdValid = false;
                for (Id cid : authInfo) {
                    AuthenticationProvider ap =
                        ProviderRegistry.getProvider(cid.getScheme());
                    if (ap == null) {
                        LOG.error("Missing AuthenticationProvider for "
                            + cid.getScheme());
                    } else if (ap.isAuthenticated()) {
                        authIdValid = true;
                        rv.add(new ACL(a.getPerms(), cid));
                    }
                }
                if (!authIdValid) {
                    throw new KeeperException.InvalidACLException(path);
                }
            } else {
                AuthenticationProvider ap = ProviderRegistry.getProvider(id.getScheme());
                if (ap == null || !ap.isValid(id.getId())) {
                    throw new KeeperException.InvalidACLException(path);
                }
                rv.add(a);
            }
        }
        return rv;
    }

    // submittedRequests为一个PreRequestProcessor内部队列，SyncRequestProcessor
    public void processRequest(Request request) {
        submittedRequests.add(request);
    }

    public void shutdown() {
        LOG.info("Shutting down");
        submittedRequests.clear();
        submittedRequests.add(Request.requestOfDeath);
        nextProcessor.shutdown();
    }
}
