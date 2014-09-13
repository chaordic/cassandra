package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.utils.Pair;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class MostOverlappingCompactionStrategy extends AbstractCompactionStrategy {

    public MostOverlappingCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
    }

    @Override
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore) {
        if (!isEnabled())
            return null;

        while (true)
        {
            List<SSTableReader> hottestBucket = getNextBackgroundSSTables(gcBefore);

            if (hottestBucket.isEmpty())
                return null;

            if (cfs.getDataTracker().markCompacting(hottestBucket))
                return new CompactionTask(cfs, hottestBucket, gcBefore, false);
        }
    }

    public static List<Pair<SSTableReader, Long>> createSSTableAndLengthPairs(Iterable<SSTableReader> sstables)
    {
        List<Pair<SSTableReader, Long>> sstableLengthPairs = new ArrayList<Pair<SSTableReader, Long>>(Iterables.size(sstables));
        for(SSTableReader sstable : sstables)
            sstableLengthPairs.add(Pair.create(sstable, sstable.onDiskLength()));
        return sstableLengthPairs;
    }

    private static Pair<Set<SSTableReader>, Set<SSTableReader>> splitInRepairedAndUnrepaired(Iterable<SSTableReader> candidates)
    {
        Set<SSTableReader> repaired = new HashSet<>();
        Set<SSTableReader> unRepaired = new HashSet<>();
        for(SSTableReader candidate : candidates)
        {
            if (!candidate.isRepaired())
                unRepaired.add(candidate);
            else
                repaired.add(candidate);
        }
        return Pair.create(repaired, unRepaired);
    }


    private List<SSTableReader> getNextBackgroundSSTables(final int gcBefore)
    {
        if (!isEnabled())
            return Collections.emptyList();

        Iterable<SSTableReader> candidates = filterSuspectSSTables(cfs.getUncompactingSSTables());
        List<SSTableReader> compactList = Collections.emptyList();
        SSTableReader[] candidateArr = Iterables.toArray(candidates, SSTableReader.class);
        long maxOverlappingKeys = 0;

        for (int i=0 ; i < candidateArr.length; i++) {
            for (int j=i + 1; j < candidateArr.length; j++) {
                SSTableReader candidate1 = candidateArr[i];
                SSTableReader candidate2 = candidateArr[j];

                List<SSTableReader> mergeList = Arrays.<SSTableReader>asList(candidate1, candidate2);
                long unionCount = SSTableReader.getApproximateKeyCount(mergeList);

                long candidate1Keys = SSTableReader.getApproximateKeyCount(Collections.singletonList(candidate1));
                long candidate2Keys = SSTableReader.getApproximateKeyCount(Collections.singletonList(candidate2));

                long totalKeys = candidate1Keys + candidate2Keys;
                long overlappingKeys = totalKeys - unionCount;

                if (overlappingKeys > maxOverlappingKeys) {
                    maxOverlappingKeys = overlappingKeys;
                    compactList = mergeList;
                }
            }
        }
        return compactList;
    }

    public Collection<AbstractCompactionTask> getMaximalTask(final int gcBefore)
    {
        Iterable<SSTableReader> allSSTables = cfs.markAllCompacting();
        if (allSSTables == null || Iterables.isEmpty(allSSTables))
            return null;
        Set<SSTableReader> sstables = Sets.newHashSet(allSSTables);
        Set<SSTableReader> repaired = new HashSet<>();
        Set<SSTableReader> unrepaired = new HashSet<>();
        for (SSTableReader sstable : sstables)
        {
            if (sstable.isRepaired())
                repaired.add(sstable);
            else
                unrepaired.add(sstable);
        }
        return Arrays.<AbstractCompactionTask>asList(new CompactionTask(cfs, repaired, gcBefore, false), new CompactionTask(cfs, unrepaired, gcBefore, false));
    }

    @Override
    public AbstractCompactionTask getUserDefinedTask(
            Collection<SSTableReader> sstables, int gcBefore) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getEstimatedRemainingTasks() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }

    /**
     * Check if given sstable is worth dropping tombstones at gcBefore.
     * Check is skipped if tombstone_compaction_interval time does not elapse since sstable creation and returns false.
     *
     * @param sstable SSTable to check
     * @param gcBefore time to drop tombstones
     * @return true if given sstable's tombstones are expected to be removed
     */
    protected boolean worthDroppingTombstones(SSTableReader sstable, int gcBefore)
    {
        // since we use estimations to calculate, there is a chance that compaction will not drop tombstones actually.
        // if that happens we will end up in infinite compaction loop, so first we check enough if enough time has
        // elapsed since SSTable created.
        if (System.currentTimeMillis() < sstable.getCreationTimeFor(Component.DATA) + tombstoneCompactionInterval * 1000)
           return false;

        double droppableRatio = sstable.getEstimatedDroppableTombstoneRatio(gcBefore);
        if (droppableRatio <= tombstoneThreshold)
            return false;

        //sstable range overlap check is disabled. See CASSANDRA-6563.
        if (uncheckedTombstoneCompaction)
            return true;

        Set<SSTableReader> overlaps = cfs.getOverlappingSSTables(Collections.singleton(sstable));
        if (overlaps.isEmpty())
        {
            // there is no overlap, tombstones are safely droppable
            return true;
        }
        else if (CompactionController.getFullyExpiredSSTables(cfs, Collections.singleton(sstable), overlaps, gcBefore).size() > 0)
        {
            return true;
        }
        else
        {
            // what percentage of columns do we expect to compact outside of overlap?
            if (sstable.getIndexSummarySize() < 2)
            {
                // we have too few samples to estimate correct percentage
                return false;
            }

            // first, calculate estimated keys that do not overlap
            long keys = sstable.estimatedKeys();
            Set<Range<Token>> ranges = new HashSet<Range<Token>>(overlaps.size());
            for (SSTableReader overlap : overlaps)
                ranges.add(new Range<Token>(overlap.first.getToken(), overlap.last.getToken(), overlap.partitioner));
            long remainingKeys = keys - sstable.estimatedKeysForRanges(ranges);
            // next, calculate what percentage of columns we have within those keys
            long columns = sstable.getEstimatedColumnCount().mean() * remainingKeys;
            double remainingColumnsRatio = ((double) columns) / (sstable.getEstimatedColumnCount().count() * sstable.getEstimatedColumnCount().mean());

            // return if we still expect to have droppable tombstones in rest of columns
            return remainingColumnsRatio * droppableRatio > tombstoneThreshold;
        }
    }
}
