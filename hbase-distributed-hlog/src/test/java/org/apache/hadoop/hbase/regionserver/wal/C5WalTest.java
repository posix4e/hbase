package org.apache.hadoop.hbase.regionserver.wal;

import c5db.interfaces.GeneralizedReplicationService;
import c5db.interfaces.replication.GeneralizedReplicator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static c5db.FutureActions.returnFutureWithValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;

public class C5WalTest {
  // Constants (names, HRegionInfo, IDs, ...) are declared at the bottom of the class.

  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery();

  private final GeneralizedReplicationService replicationService = context.mock(GeneralizedReplicationService.class);
  private final GeneralizedReplicator replicator = context.mock(GeneralizedReplicator.class);
  private C5Wal c5Wal;

  @Before
  public void setupOverallExpectationsAndCreateWal() {
    context.checking(new Expectations() {{
      allowing(replicationService).createReplicator(with(any(String.class)), with(any(Collection.class)));
//      oneOf(replicationService).stopAndWait();
    }});

    c5Wal = new C5Wal(replicationService, QUORUM_DIRECTORY);
  }

  @After
  public void closeAndDeleteWal() throws Exception {
    c5Wal.closeAndDelete();
  }

  @Test
  public void assignsTheSameQuorumIdToAllReplicasOfAGivenRegion() {
    HRegionInfo firstReplicaRegionInfo = new HRegionInfo(REGION_FOR_TABLE_A, replicaId(1));
    HRegionInfo secondReplicaRegionInfo = new HRegionInfo(REGION_FOR_TABLE_A, replicaId(2));

    assertThat(C5Wal.getQuorumIdForRegion(firstReplicaRegionInfo),
        is(equalTo(C5Wal.getQuorumIdForRegion(secondReplicaRegionInfo))));
  }

  @Test
  public void assignsDifferentQuorumIdsToDifferentRegion() {
    HRegionInfo firstRegionInfo = REGION_FOR_TABLE_A;
    HRegionInfo secondRegionInfo = REGION_FOR_TABLE_B;

    assertThat(C5Wal.getQuorumIdForRegion(firstRegionInfo),
        is(not(equalTo(C5Wal.getQuorumIdForRegion(secondRegionInfo)))));
  }

  @Test
  public void replicatesAppendedEntries() throws Exception {
    final long c5SequenceId = 7;

    context.checking(new Expectations() {{
      oneOf(replicationService).createReplicator(QUORUM_ID_A, NODE_LIST_A);
      will(returnValue(replicator));

      oneOf(replicator).replicate(with(anyData()));
      will(returnFutureWithValue(c5SequenceId));
    }});

    AtomicLong sequenceId = new AtomicLong(47);
    long nextSequenceId = sequenceId.get() + 1;

    HLogKey key = hLogKeyForRegion(REGION_FOR_TABLE_A);
    List<Cell> memstoreKVs = Lists.newArrayList(aKeyValue());

    c5Wal.appendNoSync(DESCRIPTOR_FOR_TABLE_A, REGION_FOR_TABLE_A, key, aWalEdit(), sequenceId, false, memstoreKVs);

    assertThat(key.getLogSeqNum(), is(equalTo(nextSequenceId)));
    assertThat(memstoreKVs.get(0).getSequenceId(), is(equalTo(nextSequenceId)));
  }


  private static final long NODE_ID = 1L;

  private static final TableName TABLE_NAME_A = TableName.valueOf("c5-wal-test-table-a");
  private static final HTableDescriptor DESCRIPTOR_FOR_TABLE_A = new HTableDescriptor(TABLE_NAME_A);
  private static final HRegionInfo REGION_FOR_TABLE_A = new HRegionInfo(TABLE_NAME_A);
  private static final String QUORUM_ID_A = C5Wal.getQuorumIdForRegion(REGION_FOR_TABLE_A);

  private static final TableName TABLE_NAME_B = TableName.valueOf("c5-wal-test-table-b");
  private static final HRegionInfo REGION_FOR_TABLE_B = new HRegionInfo(TABLE_NAME_B);
  private static final String QUORUM_ID_B = C5Wal.getQuorumIdForRegion(REGION_FOR_TABLE_B);

  private static final Map<String, List<Long>> QUORUM_DIRECTORY =
      ImmutableMap.<String, List<Long>>builder()
          .put(QUORUM_ID_A, Lists.newArrayList(1L, 2L, 3L))
          .put(QUORUM_ID_B, Lists.newArrayList(1L, 4L, 5L))
          .build();

  private static final List<Long> NODE_LIST_A = QUORUM_DIRECTORY.get(QUORUM_ID_A);

  private static int replicaId(int replicaId) {
    return replicaId;
  }

  private static WALEdit aWalEdit() {
    WALEdit edits = new WALEdit();
    edits.add(aKeyValue()).add(aKeyValue());
    return edits;
  }

  private static Cell aKeyValue() {
    byte[] row = "row".getBytes();
    byte[] family = "f".getBytes();
    byte[] qualifier = "qualifier".getBytes();
    byte[] value = "value".getBytes();
    return new KeyValue(row, family, qualifier, value);
  }

  private static HLogKey hLogKeyForRegion(HRegionInfo hRegionInfo) {
    return new HLogKey(hRegionInfo.getEncodedNameAsBytes(), hRegionInfo.getTable());
  }

  private Matcher<List<ByteBuffer>> anyData() {
    return Matchers.instanceOf(List.class);
  }
}
