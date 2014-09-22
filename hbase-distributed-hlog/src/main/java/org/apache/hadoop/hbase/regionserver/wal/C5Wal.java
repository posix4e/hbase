package org.apache.hadoop.hbase.regionserver.wal;

import c5db.ReplicatorConstants;
import c5db.interfaces.GeneralizedReplicationService;
import c5db.interfaces.replication.GeneralizedReplicator;
import c5db.util.ExceptionHandlingBatchExecutor;
import c5db.util.FiberSupplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

public class C5Wal implements HLog {
  private static final Log LOG = LogFactory.getLog(C5Wal.class);

  private static final int PORT = ReplicatorConstants.REPLICATOR_PORT_MIN;
  private static final int NUMBER_OF_FIBER_THREADS = 3;

  private final ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_FIBER_THREADS);
  private final GeneralizedReplicationService replicationService;
  private final PoolFiberFactory poolFiberFactory = new PoolFiberFactory(executorService);
  private final FiberSupplier fiberSupplier = new PoolFiberSupplier(poolFiberFactory);
  private final Fiber fiber;

  private final ImmutableMap<String, List<Long>> quorumIdToNodeIdsMap;

  private final Map<String, ListenableFuture<GeneralizedReplicator>> replicatorFutures =
      new ConcurrentHashMap<>();


  public C5Wal(GeneralizedReplicationService replicationService,
               Map<String, List<Long>> quorumIdToNodeIdMap) {
    this.quorumIdToNodeIdsMap = ImmutableMap.copyOf(quorumIdToNodeIdMap);
    this.fiber = fiberSupplier.getNewFiber(new ThrowableLogger());
    this.replicationService = replicationService;

    // TODO don't start in the constructor; and use startAsync instead of startAndWait
    for (Map.Entry<String, List<Long>> nodesToBootStrap : quorumIdToNodeIdMap.entrySet()) {
      LOG.error("Starting replicator for:" + nodesToBootStrap.getKey());
      ListenableFuture<GeneralizedReplicator> replicator =
          replicationService.createReplicator(nodesToBootStrap.getKey(),
              nodesToBootStrap.getValue());
      replicatorFutures.put(nodesToBootStrap.getKey(), replicator);
    }
  }

  private void shutdown() {
    // TODO use stopAsync
    for (Map.Entry<String, ListenableFuture<GeneralizedReplicator>> replicator
        : replicatorFutures.entrySet()) {
      LOG.error("Should stopping replicator for:" + replicator.getKey());

    }
    poolFiberFactory.dispose();
    executorService.shutdown();
  }

  private ListenableFuture<GeneralizedReplicator> getReplicatorForRegion(HRegionInfo hRegionInfo) {
    final String quorumId = getQuorumIdForRegion(hRegionInfo);

    replicatorFutures.computeIfAbsent(quorumId, new Function<String, ListenableFuture<GeneralizedReplicator>>() {
      @Override
      public ListenableFuture<GeneralizedReplicator> apply(String s) {
        List<Long> peerIds = quorumIdToNodeIdsMap.get(quorumId);
        return replicationService.createReplicator(quorumId, peerIds);
      }
    });

    return replicatorFutures.get(quorumId);
  }

  static String getQuorumIdForRegion(HRegionInfo hRegionInfo) {
    // Zero out the replica field so that all replicas for a given region will have the same quorum ID
    return new HRegionInfo(hRegionInfo, 0).getRegionNameAsString();
  }

  @Override
  public void registerWALActionsListener(WALActionsListener listener) {
    LOG.error("unsupported registerWALActionsListener, no-op");
  }

  @Override
  public boolean unregisterWALActionsListener(WALActionsListener listener) {
    LOG.error("unsupported unregisterWALActionsListener, returning false");
    return false;
  }

  @Override
  public long getFilenum() {
    // TODO
    return 0;
  }

  @Override
  public int getNumLogFiles() {
    // TODO
    return 0;
  }

  @Override
  public long getLogFileSize() {
    // TODO
    return 0;
  }

  @Override
  public byte[][] rollWriter() throws IOException {
    // TODO
    return new byte[0][];
  }

  @Override
  public byte[][] rollWriter(boolean force) throws IOException {
    return rollWriter();
  }

  @Override
  public void close() throws IOException {
    shutdown();
  }

  @Override
  public void closeAndDelete() throws IOException {
    close();
    // TODO delete?
  }

  @Override
  public void append(HRegionInfo info,
                     TableName tableName,
                     WALEdit edits,
                     long now,
                     HTableDescriptor htd,
                     AtomicLong sequenceId) throws IOException {
  }

  @Override
  public long postAppend(Entry entry, long elapsedTime) {
    return 0;
  }

  @Override
  public void postSync(long timeInMillis, int handlerSyncs) {
  }

  @Override
  public long appendNoSync(HRegionInfo info,
                           TableName tableName,
                           WALEdit edits,
                           List<UUID> clusterIds,
                           long now,
                           HTableDescriptor htd,
                           AtomicLong sequenceId,
                           boolean isInMemstore,
                           long nonceGroup,
                           long nonce) throws IOException {

    HLogKey logKey =
        new HLogKey(info.getEncodedNameAsBytes(), tableName, now, clusterIds, nonceGroup, nonce);
    return append(htd, info, logKey, edits, sequenceId, false, isInMemstore, null);
  }

  private long append(HTableDescriptor htd,
                      final HRegionInfo hri,
                      final HLogKey key,
                      WALEdit edits,
                      AtomicLong sequenceId,
                      boolean sync,
                      boolean inMemstore,
                      List<Cell> memstoreCells) {

  return 0;


  }


  @Override
  public long appendNoSync(HTableDescriptor htd,
                           HRegionInfo info,
                           HLogKey key,
                           WALEdit edits,
                           AtomicLong sequenceId,
                           boolean inMemstore,
                           List<Cell> memstoreCells) throws IOException {
    return append(htd, info, key, edits, sequenceId, false, inMemstore, memstoreCells);
  }

  @Override
  public void hsync() throws IOException {
    this.sync();
  }

  @Override
  public void hflush() throws IOException {
    this.sync();
  }

  @Override
  public void sync() throws IOException {
    // TODO
  }

  @Override
  public void sync(long txid) throws IOException {
    this.sync();
  }

  @Override
  public boolean startCacheFlush(byte[] encodedRegionName) {
    LOG.error("unsupported startCacheFlush, returning false");
    return false;
  }

  @Override
  public void completeCacheFlush(byte[] encodedRegionName) {
    LOG.error("unsupported completeCacheFlush, no-op");
  }

  @Override
  public void abortCacheFlush(byte[] encodedRegionName) {
    LOG.error("unsupported abortCacheFlush, no-op");
  }

  @Override
  public WALCoprocessorHost getCoprocessorHost() {
    return null;
  }

  @Override
  public boolean isLowReplicationRollEnabled() {
    return false;
  }

  @Override
  public long getEarliestMemstoreSeqNum(byte[] encodedRegionName) {
    return 0;
  }


  private class PoolFiberSupplier implements FiberSupplier {
    private final PoolFiberFactory poolFiberFactory;

    private PoolFiberSupplier(PoolFiberFactory poolFiberFactory) {
      this.poolFiberFactory = poolFiberFactory;
    }

    @Override
    public Fiber getNewFiber(Consumer<Throwable> throwableConsumer) {
      return poolFiberFactory.create(new ExceptionHandlingBatchExecutor(throwableConsumer));
    }
  }

  private static class ThrowableLogger implements Consumer<Throwable> {
    @Override
    public void accept(Throwable throwable) {
      LOG.error("error in Fiber issued by C5Wal", throwable);
    }
  }

}
