/*
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
package org.apache.cassandra.service;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.base.Predicate;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.*;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.metrics.*;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.paxos.*;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.apache.cassandra.utils.*;

public class StorageProxy implements StorageProxyMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=StorageProxy";
    private static final Logger logger = LoggerFactory.getLogger(StorageProxy.class);

    public static final String UNREACHABLE = "UNREACHABLE";

    private static final WritePerformer standardWritePerformer;
    private static final WritePerformer counterWritePerformer;
    private static final WritePerformer counterWriteOnCoordinatorPerformer;

    public static final StorageProxy instance = new StorageProxy();

    private static volatile int maxHintsInProgress = 128 * FBUtilities.getAvailableProcessors();
    private static final CacheLoader<InetAddress, AtomicInteger> hintsInProgress = new CacheLoader<InetAddress, AtomicInteger>()
    {
        public AtomicInteger load(InetAddress inetAddress)
        {
            return new AtomicInteger(0);
        }
    };
    private static final ClientRequestMetrics readMetrics = new ClientRequestMetrics("Read");
    private static final ClientRequestMetrics rangeMetrics = new ClientRequestMetrics("RangeSlice");
    private static final ClientRequestMetrics writeMetrics = new ClientRequestMetrics("Write");
    private static final CASClientRequestMetrics casWriteMetrics = new CASClientRequestMetrics("CASWrite");
    private static final CASClientRequestMetrics casReadMetrics = new CASClientRequestMetrics("CASRead");

    private static final double CONCURRENT_SUBREQUESTS_MARGIN = 0.10;

    private StorageProxy() {}

    static
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(instance, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        standardWritePerformer = new WritePerformer()
        {
            public void apply(IMutation mutation,
                              Iterable<InetAddress> targets,
                              AbstractWriteResponseHandler<IMutation> responseHandler,
                              String localDataCenter,
                              ConsistencyLevel consistency_level)
            throws OverloadedException
            {
                assert mutation instanceof Mutation;
                sendToHintedEndpoints((Mutation) mutation, targets, responseHandler, localDataCenter);
            }
        };

        /*
         * We execute counter writes in 2 places: either directly in the coordinator node if it is a replica, or
         * in CounterMutationVerbHandler on a replica othewise. The write must be executed on the COUNTER_MUTATION stage
         * but on the latter case, the verb handler already run on the COUNTER_MUTATION stage, so we must not execute the
         * underlying on the stage otherwise we risk a deadlock. Hence two different performer.
         */
        counterWritePerformer = new WritePerformer()
        {
            public void apply(IMutation mutation,
                              Iterable<InetAddress> targets,
                              AbstractWriteResponseHandler<IMutation> responseHandler,
                              String localDataCenter,
                              ConsistencyLevel consistencyLevel)
            {
                counterWriteTask(mutation, targets, responseHandler, localDataCenter).run();
            }
        };

        counterWriteOnCoordinatorPerformer = new WritePerformer()
        {
            public void apply(IMutation mutation,
                              Iterable<InetAddress> targets,
                              AbstractWriteResponseHandler<IMutation> responseHandler,
                              String localDataCenter,
                              ConsistencyLevel consistencyLevel)
            {
                StageManager.getStage(Stage.COUNTER_MUTATION)
                            .execute(counterWriteTask(mutation, targets, responseHandler, localDataCenter));
            }
        };
    }

    /**
     * Apply @param updates if and only if the current values in the row for @param key
     * match the provided @param conditions.  The algorithm is "raw" Paxos: that is, Paxos
     * minus leader election -- any node in the cluster may propose changes for any row,
     * which (that is, the row) is the unit of values being proposed, not single columns.
     *
     * The Paxos cohort is only the replicas for the given key, not the entire cluster.
     * So we expect performance to be reasonable, but CAS is still intended to be used
     * "when you really need it," not for all your updates.
     *
     * There are three phases to Paxos:
     *  1. Prepare: the coordinator generates a ballot (timeUUID in our case) and asks replicas to (a) promise
     *     not to accept updates from older ballots and (b) tell us about the most recent update it has already
     *     accepted.
     *  2. Accept: if a majority of replicas reply, the coordinator asks replicas to accept the value of the
     *     highest proposal ballot it heard about, or a new value if no in-progress proposals were reported.
     *  3. Commit (Learn): if a majority of replicas acknowledge the accept request, we can commit the new
     *     value.
     *
     *  Commit procedure is not covered in "Paxos Made Simple," and only briefly mentioned in "Paxos Made Live,"
     *  so here is our approach:
     *   3a. The coordinator sends a commit message to all replicas with the ballot and value.
     *   3b. Because of 1-2, this will be the highest-seen commit ballot.  The replicas will note that,
     *       and send it with subsequent promise replies.  This allows us to discard acceptance records
     *       for successfully committed replicas, without allowing incomplete proposals to commit erroneously
     *       later on.
     *
     *  Note that since we are performing a CAS rather than a simple update, we perform a read (of committed
     *  values) between the prepare and accept phases.  This gives us a slightly longer window for another
     *  coordinator to come along and trump our own promise with a newer one but is otherwise safe.
     *
     * @param keyspaceName the keyspace for the CAS
     * @param cfName the column family for the CAS
     * @param key the row key for the row to CAS
     * @param request the conditions for the CAS to apply as well as the update to perform if the conditions hold.
     * @param consistencyForPaxos the consistency for the paxos prepare and propose round. This can only be either SERIAL or LOCAL_SERIAL.
     * @param consistencyForCommit the consistency for write done during the commit phase. This can be anything, except SERIAL or LOCAL_SERIAL.
     *
     * @return null if the operation succeeds in updating the row, or the current values corresponding to conditions.
     * (since, if the CAS doesn't succeed, it means the current value do not match the conditions).
     */
    public static RowIterator cas(String keyspaceName,
                                  String cfName,
                                  DecoratedKey key,
                                  CASRequest request,
                                  ConsistencyLevel consistencyForPaxos,
                                  ConsistencyLevel consistencyForCommit,
                                  ClientState state)
    throws UnavailableException, IsBootstrappingException, RequestFailureException, RequestTimeoutException, InvalidRequestException
    {
        final long start = System.nanoTime();
        int contentions = 0;
        try
        {
            consistencyForPaxos.validateForCas();
            consistencyForCommit.validateForCasCommit(keyspaceName);

            CFMetaData metadata = Schema.instance.getCFMetaData(keyspaceName, cfName);

            long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getCasContentionTimeout());
            while (System.nanoTime() - start < timeout)
            {
                // for simplicity, we'll do a single liveness check at the start of each attempt
                Pair<List<InetAddress>, Integer> p = getPaxosParticipants(metadata, key, consistencyForPaxos);
                List<InetAddress> liveEndpoints = p.left;
                int requiredParticipants = p.right;

                final Pair<UUID, Integer> pair = beginAndRepairPaxos(start, key, metadata, liveEndpoints, requiredParticipants, consistencyForPaxos, consistencyForCommit, true, state);
                final UUID ballot = pair.left;
                contentions += pair.right;

                // read the current values and check they validate the conditions
                Tracing.trace("Reading existing values for CAS precondition");
                SinglePartitionReadCommand readCommand = request.readCommand(FBUtilities.nowInSeconds());
                ConsistencyLevel readConsistency = consistencyForPaxos == ConsistencyLevel.LOCAL_SERIAL ? ConsistencyLevel.LOCAL_QUORUM : ConsistencyLevel.QUORUM;

                FilteredPartition current;
                try (RowIterator rowIter = readOne(readCommand, readConsistency))
                {
                    current = FilteredPartition.create(rowIter);
                }

                if (!request.appliesTo(current))
                {
                    Tracing.trace("CAS precondition does not match current values {}", current);
                    casWriteMetrics.conditionNotMet.inc();
                    return current.rowIterator();
                }

                // finish the paxos round w/ the desired updates
                // TODO turn null updates into delete?
                PartitionUpdate updates = request.makeUpdates(current);

                // Apply triggers to cas updates. A consideration here is that
                // triggers emit Mutations, and so a given trigger implementation
                // may generate mutations for partitions other than the one this
                // paxos round is scoped for. In this case, TriggerExecutor will
                // validate that the generated mutations are targetted at the same
                // partition as the initial updates and reject (via an
                // InvalidRequestException) any which aren't.
                updates = TriggerExecutor.instance.execute(updates);


                Commit proposal = Commit.newProposal(ballot, updates);
                Tracing.trace("CAS precondition is met; proposing client-requested updates for {}", ballot);
                if (proposePaxos(proposal, liveEndpoints, requiredParticipants, true, consistencyForPaxos))
                {
                    commitPaxos(proposal, consistencyForCommit);
                    Tracing.trace("CAS successful");
                    return null;
                }

                Tracing.trace("Paxos proposal not accepted (pre-empted by a higher ballot)");
                contentions++;
                Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                // continue to retry
            }

            throw new WriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, consistencyForPaxos.blockFor(Keyspace.open(keyspaceName)));
        }
        catch (WriteTimeoutException|ReadTimeoutException e)
        {
            casWriteMetrics.timeouts.mark();
            throw e;
        }
        catch (WriteFailureException|ReadFailureException e)
        {
            casWriteMetrics.failures.mark();
            throw e;
        }
        catch(UnavailableException e)
        {
            casWriteMetrics.unavailables.mark();
            throw e;
        }
        finally
        {
            if(contentions > 0)
                casWriteMetrics.contention.update(contentions);
            casWriteMetrics.addNano(System.nanoTime() - start);
        }
    }

    private static Predicate<InetAddress> sameDCPredicateFor(final String dc)
    {
        final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        return new Predicate<InetAddress>()
        {
            public boolean apply(InetAddress host)
            {
                return dc.equals(snitch.getDatacenter(host));
            }
        };
    }

    private static Pair<List<InetAddress>, Integer> getPaxosParticipants(CFMetaData cfm, DecoratedKey key, ConsistencyLevel consistencyForPaxos) throws UnavailableException
    {
        Token tk = key.getToken();
        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(cfm.ksName, tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, cfm.ksName);
        if (consistencyForPaxos == ConsistencyLevel.LOCAL_SERIAL)
        {
            // Restrict naturalEndpoints and pendingEndpoints to node in the local DC only
            String localDc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
            Predicate<InetAddress> isLocalDc = sameDCPredicateFor(localDc);
            naturalEndpoints = ImmutableList.copyOf(Iterables.filter(naturalEndpoints, isLocalDc));
            pendingEndpoints = ImmutableList.copyOf(Iterables.filter(pendingEndpoints, isLocalDc));
        }
        int participants = pendingEndpoints.size() + naturalEndpoints.size();
        int requiredParticipants = participants / 2 + 1; // See CASSANDRA-8346, CASSANDRA-833
        List<InetAddress> liveEndpoints = ImmutableList.copyOf(Iterables.filter(Iterables.concat(naturalEndpoints, pendingEndpoints), IAsyncCallback.isAlive));
        if (liveEndpoints.size() < requiredParticipants)
            throw new UnavailableException(consistencyForPaxos, requiredParticipants, liveEndpoints.size());

        // We cannot allow CAS operations with 2 or more pending endpoints, see #8346.
        // Note that we fake an impossible number of required nodes in the unavailable exception
        // to nail home the point that it's an impossible operation no matter how many nodes are live.
        if (pendingEndpoints.size() > 1)
            throw new UnavailableException(String.format("Cannot perform LWT operation as there is more than one (%d) pending range movement", pendingEndpoints.size()),
                                           consistencyForPaxos,
                                           participants + 1,
                                           liveEndpoints.size());

        return Pair.create(liveEndpoints, requiredParticipants);
    }

    /**
     * begin a Paxos session by sending a prepare request and completing any in-progress requests seen in the replies
     *
     * @return the Paxos ballot promised by the replicas if no in-progress requests were seen and a quorum of
     * nodes have seen the mostRecentCommit.  Otherwise, return null.
     */
    private static Pair<UUID, Integer> beginAndRepairPaxos(long start,
                                                           DecoratedKey key,
                                                           CFMetaData metadata,
                                                           List<InetAddress> liveEndpoints,
                                                           int requiredParticipants,
                                                           ConsistencyLevel consistencyForPaxos,
                                                           ConsistencyLevel consistencyForCommit,
                                                           final boolean isWrite,
                                                           ClientState state)
    throws WriteTimeoutException, WriteFailureException
    {
        long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getCasContentionTimeout());

        PrepareCallback summary = null;
        int contentions = 0;
        while (System.nanoTime() - start < timeout)
        {
            // We want a timestamp that is guaranteed to be unique for that node (so that the ballot is globally unique), but if we've got a prepare rejected
            // already we also want to make sure we pick a timestamp that has a chance to be promised, i.e. one that is greater that the most recently known
            // in progress (#5667). Lastly, we don't want to use a timestamp that is older than the last one assigned by ClientState or operations may appear
            // out-of-order (#7801).
            long minTimestampMicrosToUse = summary == null ? Long.MIN_VALUE : 1 + UUIDGen.microsTimestamp(summary.mostRecentInProgressCommit.ballot);
            long ballotMicros = state.getTimestamp(minTimestampMicrosToUse);
            UUID ballot = UUIDGen.getTimeUUIDFromMicros(ballotMicros);

            // prepare
            Tracing.trace("Preparing {}", ballot);
            Commit toPrepare = Commit.newPrepare(key, metadata, ballot);
            summary = preparePaxos(toPrepare, liveEndpoints, requiredParticipants, consistencyForPaxos);
            if (!summary.promised)
            {
                Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
                contentions++;
                // sleep a random amount to give the other proposer a chance to finish
                Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                continue;
            }

            Commit inProgress = summary.mostRecentInProgressCommitWithUpdate;
            Commit mostRecent = summary.mostRecentCommit;

            // If we have an in-progress ballot greater than the MRC we know, then it's an in-progress round that
            // needs to be completed, so do it.
            if (!inProgress.update.isEmpty() && inProgress.isAfter(mostRecent))
            {
                Tracing.trace("Finishing incomplete paxos round {}", inProgress);
                if(isWrite)
                    casWriteMetrics.unfinishedCommit.inc();
                else
                    casReadMetrics.unfinishedCommit.inc();
                Commit refreshedInProgress = Commit.newProposal(ballot, inProgress.update);
                if (proposePaxos(refreshedInProgress, liveEndpoints, requiredParticipants, false, consistencyForPaxos))
                {
                    try
                    {
                        commitPaxos(refreshedInProgress, consistencyForCommit);
                    }
                    catch (WriteTimeoutException e)
                    {
                        // We're still doing preparation for the paxos rounds, so we want to use the CAS (see CASSANDRA-8672)
                        throw new WriteTimeoutException(WriteType.CAS, e.consistency, e.received, e.blockFor);
                    }
                }
                else
                {
                    Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
                    // sleep a random amount to give the other proposer a chance to finish
                    contentions++;
                    Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                }
                continue;
            }

            // To be able to propose our value on a new round, we need a quorum of replica to have learn the previous one. Why is explained at:
            // https://issues.apache.org/jira/browse/CASSANDRA-5062?focusedCommentId=13619810&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-13619810)
            // Since we waited for quorum nodes, if some of them haven't seen the last commit (which may just be a timing issue, but may also
            // mean we lost messages), we pro-actively "repair" those nodes, and retry.
            Iterable<InetAddress> missingMRC = summary.replicasMissingMostRecentCommit();
            if (Iterables.size(missingMRC) > 0)
            {
                Tracing.trace("Repairing replicas that missed the most recent commit");
                sendCommit(mostRecent, missingMRC);
                // TODO: provided commits don't invalid the prepare we just did above (which they don't), we could just wait
                // for all the missingMRC to acknowledge this commit and then move on with proposing our value. But that means
                // adding the ability to have commitPaxos block, which is exactly CASSANDRA-5442 will do. So once we have that
                // latter ticket, we can pass CL.ALL to the commit above and remove the 'continue'.
                continue;
            }

            return Pair.create(ballot, contentions);
        }

        throw new WriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, consistencyForPaxos.blockFor(Keyspace.open(metadata.ksName)));
    }

    /**
     * Unlike commitPaxos, this does not wait for replies
     */
    private static void sendCommit(Commit commit, Iterable<InetAddress> replicas)
    {
        MessageOut<Commit> message = new MessageOut<Commit>(MessagingService.Verb.PAXOS_COMMIT, commit, Commit.serializer);
        for (InetAddress target : replicas)
            MessagingService.instance().sendOneWay(message, target);
    }

    private static PrepareCallback preparePaxos(Commit toPrepare, List<InetAddress> endpoints, int requiredParticipants, ConsistencyLevel consistencyForPaxos)
    throws WriteTimeoutException
    {
        PrepareCallback callback = new PrepareCallback(toPrepare.update.partitionKey(), toPrepare.update.metadata(), requiredParticipants, consistencyForPaxos);
        MessageOut<Commit> message = new MessageOut<Commit>(MessagingService.Verb.PAXOS_PREPARE, toPrepare, Commit.serializer);
        for (InetAddress target : endpoints)
            MessagingService.instance().sendRR(message, target, callback);
        callback.await();
        return callback;
    }

    private static boolean proposePaxos(Commit proposal, List<InetAddress> endpoints, int requiredParticipants, boolean timeoutIfPartial, ConsistencyLevel consistencyLevel)
    throws WriteTimeoutException
    {
        ProposeCallback callback = new ProposeCallback(endpoints.size(), requiredParticipants, !timeoutIfPartial, consistencyLevel);
        MessageOut<Commit> message = new MessageOut<Commit>(MessagingService.Verb.PAXOS_PROPOSE, proposal, Commit.serializer);
        for (InetAddress target : endpoints)
            MessagingService.instance().sendRR(message, target, callback);

        callback.await();

        if (callback.isSuccessful())
            return true;

        if (timeoutIfPartial && !callback.isFullyRefused())
            throw new WriteTimeoutException(WriteType.CAS, consistencyLevel, callback.getAcceptCount(), requiredParticipants);

        return false;
    }

    private static void commitPaxos(Commit proposal, ConsistencyLevel consistencyLevel) throws WriteTimeoutException, WriteFailureException
    {
        boolean shouldBlock = consistencyLevel != ConsistencyLevel.ANY;
        Keyspace keyspace = Keyspace.open(proposal.update.metadata().ksName);

        Token tk = proposal.update.partitionKey().getToken();
        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspace.getName(), tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspace.getName());

        AbstractWriteResponseHandler<Commit> responseHandler = null;
        if (shouldBlock)
        {
            AbstractReplicationStrategy rs = keyspace.getReplicationStrategy();
            responseHandler = rs.getWriteResponseHandler(naturalEndpoints, pendingEndpoints, consistencyLevel, null, WriteType.SIMPLE);
        }

        MessageOut<Commit> message = new MessageOut<Commit>(MessagingService.Verb.PAXOS_COMMIT, proposal, Commit.serializer);
        for (InetAddress destination : Iterables.concat(naturalEndpoints, pendingEndpoints))
        {
            if (FailureDetector.instance.isAlive(destination))
            {
                if (shouldBlock)
                    MessagingService.instance().sendRRWithFailure(message, destination, responseHandler);
                else
                    MessagingService.instance().sendOneWay(message, destination);
            }
        }

        if (shouldBlock)
            responseHandler.get();
    }

    /**
     * Use this method to have these Mutations applied
     * across all replicas. This method will take care
     * of the possibility of a replica being down and hint
     * the data across to some other replica.
     *
     * @param mutations the mutations to be applied across the replicas
     * @param consistency_level the consistency level for the operation
     */
    public static void mutate(Collection<? extends IMutation> mutations, ConsistencyLevel consistency_level)
    throws UnavailableException, OverloadedException, WriteTimeoutException, WriteFailureException
    {
        Tracing.trace("Determining replicas for mutation");
        final String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());

        long startTime = System.nanoTime();
        List<AbstractWriteResponseHandler<IMutation>> responseHandlers = new ArrayList<>(mutations.size());

        try
        {
            for (IMutation mutation : mutations)
            {
                if (mutation instanceof CounterMutation)
                {
                    responseHandlers.add(mutateCounter((CounterMutation)mutation, localDataCenter));
                }
                else
                {
                    WriteType wt = mutations.size() <= 1 ? WriteType.SIMPLE : WriteType.UNLOGGED_BATCH;
                    responseHandlers.add(performWrite(mutation, consistency_level, localDataCenter, standardWritePerformer, null, wt));
                }
            }

            // wait for writes.  throws TimeoutException if necessary
            for (AbstractWriteResponseHandler<IMutation> responseHandler : responseHandlers)
            {
                responseHandler.get();
            }
        }
        catch (WriteTimeoutException|WriteFailureException ex)
        {
            if (consistency_level == ConsistencyLevel.ANY)
            {
                hintMutations(mutations);
            }
            else
            {
                if (ex instanceof WriteFailureException)
                {
                    writeMetrics.failures.mark();
                    WriteFailureException fe = (WriteFailureException)ex;
                    Tracing.trace("Write failure; received {} of {} required replies, failed {} requests",
                                  fe.received, fe.blockFor, fe.failures);
                }
                else
                {
                    writeMetrics.timeouts.mark();
                    WriteTimeoutException te = (WriteTimeoutException)ex;
                    Tracing.trace("Write timeout; received {} of {} required replies", te.received, te.blockFor);
                }
                throw ex;
            }
        }
        catch (UnavailableException e)
        {
            writeMetrics.unavailables.mark();
            Tracing.trace("Unavailable");
            throw e;
        }
        catch (OverloadedException e)
        {
            writeMetrics.unavailables.mark();
            Tracing.trace("Overloaded");
            throw e;
        }
        finally
        {
            writeMetrics.addNano(System.nanoTime() - startTime);
        }
    }

    /** hint all the mutations (except counters, which can't be safely retried).  This means
      * we'll re-hint any successful ones; doesn't seem worth it to track individual success
      * just for this unusual case.

      * @param mutations the mutations that require hints
      */
    private static void hintMutations(Collection<? extends IMutation> mutations)
    {
        for (IMutation mutation : mutations)
        {
            if (mutation instanceof CounterMutation)
                continue;

            Token tk = mutation.key().getToken();
            List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(mutation.getKeyspaceName(), tk);
            Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, mutation.getKeyspaceName());
            for (InetAddress target : Iterables.concat(naturalEndpoints, pendingEndpoints))
            {
                // local writes can timeout, but cannot be dropped (see LocalMutationRunnable and
                // CASSANDRA-6510), so there is no need to hint or retry
                if (!target.equals(FBUtilities.getBroadcastAddress()) && shouldHint(target))
                    submitHint((Mutation) mutation, target, null);
            }
        }

        Tracing.trace("Wrote hint to satisfy CL.ANY after no replicas acknowledged the write");
    }

    @SuppressWarnings("unchecked")
    public static void mutateWithTriggers(Collection<? extends IMutation> mutations,
                                          ConsistencyLevel consistencyLevel,
                                          boolean mutateAtomically)
    throws WriteTimeoutException, WriteFailureException, UnavailableException, OverloadedException, InvalidRequestException
    {
        Collection<Mutation> augmented = TriggerExecutor.instance.execute(mutations);

        if (augmented != null)
            mutateAtomically(augmented, consistencyLevel);
        else if (mutateAtomically)
            mutateAtomically((Collection<Mutation>) mutations, consistencyLevel);
        else
            mutate(mutations, consistencyLevel);
    }

    /**
     * See mutate. Adds additional steps before and after writing a batch.
     * Before writing the batch (but after doing availability check against the FD for the row replicas):
     *      write the entire batch to a batchlog elsewhere in the cluster.
     * After: remove the batchlog entry (after writing hints for the batch rows, if necessary).
     *
     * @param mutations the Mutations to be applied across the replicas
     * @param consistency_level the consistency level for the operation
     */
    public static void mutateAtomically(Collection<Mutation> mutations, ConsistencyLevel consistency_level)
    throws UnavailableException, OverloadedException, WriteTimeoutException
    {
        Tracing.trace("Determining replicas for atomic batch");
        long startTime = System.nanoTime();

        List<WriteResponseHandlerWrapper> wrappers = new ArrayList<WriteResponseHandlerWrapper>(mutations.size());
        String localDataCenter = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());

        try
        {
            // add a handler for each mutation - includes checking availability, but doesn't initiate any writes, yet
            for (Mutation mutation : mutations)
            {
                WriteResponseHandlerWrapper wrapper = wrapResponseHandler(mutation, consistency_level, WriteType.BATCH);
                // exit early if we can't fulfill the CL at this time.
                wrapper.handler.assureSufficientLiveNodes();
                wrappers.add(wrapper);
            }

            // write to the batchlog
            Collection<InetAddress> batchlogEndpoints = getBatchlogEndpoints(localDataCenter, consistency_level);
            UUID batchUUID = UUIDGen.getTimeUUID();
            syncWriteToBatchlog(mutations, batchlogEndpoints, batchUUID);

            // now actually perform the writes and wait for them to complete
            syncWriteBatchedMutations(wrappers, localDataCenter);

            // remove the batchlog entries asynchronously
            asyncRemoveFromBatchlog(batchlogEndpoints, batchUUID);
        }
        catch (UnavailableException e)
        {
            writeMetrics.unavailables.mark();
            Tracing.trace("Unavailable");
            throw e;
        }
        catch (WriteTimeoutException e)
        {
            writeMetrics.timeouts.mark();
            Tracing.trace("Write timeout; received {} of {} required replies", e.received, e.blockFor);
            throw e;
        }
        catch (WriteFailureException e)
        {
            writeMetrics.failures.mark();
            Tracing.trace("Write failure; received {} of {} required replies", e.received, e.blockFor);
            throw e;
        }
        finally
        {
            writeMetrics.addNano(System.nanoTime() - startTime);
        }
    }

    public static boolean canDoLocalRequest(InetAddress replica)
    {
        return replica.equals(FBUtilities.getBroadcastAddress());
    }


    private static void syncWriteToBatchlog(Collection<Mutation> mutations, Collection<InetAddress> endpoints, UUID uuid)
    throws WriteTimeoutException, WriteFailureException
    {
        AbstractWriteResponseHandler<IMutation> handler = new WriteResponseHandler<>(endpoints,
                                                                        Collections.<InetAddress>emptyList(),
                                                                        ConsistencyLevel.ONE,
                                                                        Keyspace.open(SystemKeyspace.NAME),
                                                                        null,
                                                                        WriteType.BATCH_LOG);

        MessageOut<Mutation> message = BatchlogManager.getBatchlogMutationFor(mutations, uuid, MessagingService.current_version)
                                                      .createMessage();
        for (InetAddress target : endpoints)
        {
            int targetVersion = MessagingService.instance().getVersion(target);
            if (canDoLocalRequest(target))
            {
                insertLocal(message.payload, handler);
            }
            else if (targetVersion == MessagingService.current_version)
            {
                MessagingService.instance().sendRR(message, target, handler, false);
            }
            else
            {
                MessagingService.instance().sendRR(BatchlogManager.getBatchlogMutationFor(mutations, uuid, targetVersion)
                                                                  .createMessage(),
                                                   target,
                                                   handler,
                                                   false);
            }
        }

        handler.get();
    }

    private static void asyncRemoveFromBatchlog(Collection<InetAddress> endpoints, UUID uuid)
    {
        AbstractWriteResponseHandler<IMutation> handler = new WriteResponseHandler<>(endpoints,
                                                                        Collections.<InetAddress>emptyList(),
                                                                        ConsistencyLevel.ANY,
                                                                        Keyspace.open(SystemKeyspace.NAME),
                                                                        null,
                                                                        WriteType.SIMPLE);
        Mutation mutation = new Mutation(SystemKeyspace.NAME, StorageService.getPartitioner().decorateKey(UUIDType.instance.decompose(uuid)));
        mutation.add(PartitionUpdate.fullPartitionDelete(SystemKeyspace.Batchlog, mutation.key(), FBUtilities.timestampMicros(), FBUtilities.nowInSeconds()));
        MessageOut<Mutation> message = mutation.createMessage();
        for (InetAddress target : endpoints)
        {
            if (canDoLocalRequest(target))
                insertLocal(message.payload, handler);
            else
                MessagingService.instance().sendRR(message, target, handler, false);
        }
    }

    private static void syncWriteBatchedMutations(List<WriteResponseHandlerWrapper> wrappers, String localDataCenter)
    throws WriteTimeoutException, OverloadedException
    {
        for (WriteResponseHandlerWrapper wrapper : wrappers)
        {
            Iterable<InetAddress> endpoints = Iterables.concat(wrapper.handler.naturalEndpoints, wrapper.handler.pendingEndpoints);
            sendToHintedEndpoints(wrapper.mutation, endpoints, wrapper.handler, localDataCenter);
        }

        for (WriteResponseHandlerWrapper wrapper : wrappers)
            wrapper.handler.get();
    }

    /**
     * Perform the write of a mutation given a WritePerformer.
     * Gather the list of write endpoints, apply locally and/or forward the mutation to
     * said write endpoint (deletaged to the actual WritePerformer) and wait for the
     * responses based on consistency level.
     *
     * @param mutation the mutation to be applied
     * @param consistency_level the consistency level for the write operation
     * @param performer the WritePerformer in charge of appliying the mutation
     * given the list of write endpoints (either standardWritePerformer for
     * standard writes or counterWritePerformer for counter writes).
     * @param callback an optional callback to be run if and when the write is
     * successful.
     */
    public static AbstractWriteResponseHandler<IMutation> performWrite(IMutation mutation,
                                                            ConsistencyLevel consistency_level,
                                                            String localDataCenter,
                                                            WritePerformer performer,
                                                            Runnable callback,
                                                            WriteType writeType)
    throws UnavailableException, OverloadedException
    {
        String keyspaceName = mutation.getKeyspaceName();
        AbstractReplicationStrategy rs = Keyspace.open(keyspaceName).getReplicationStrategy();

        Token tk = mutation.key().getToken();
        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspaceName, tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspaceName);

        AbstractWriteResponseHandler<IMutation> responseHandler = rs.getWriteResponseHandler(naturalEndpoints, pendingEndpoints, consistency_level, callback, writeType);

        // exit early if we can't fulfill the CL at this time
        responseHandler.assureSufficientLiveNodes();

        performer.apply(mutation, Iterables.concat(naturalEndpoints, pendingEndpoints), responseHandler, localDataCenter, consistency_level);
        return responseHandler;
    }

    // same as above except does not initiate writes (but does perform availability checks).
    private static WriteResponseHandlerWrapper wrapResponseHandler(Mutation mutation, ConsistencyLevel consistency_level, WriteType writeType)
    {
        AbstractReplicationStrategy rs = Keyspace.open(mutation.getKeyspaceName()).getReplicationStrategy();
        String keyspaceName = mutation.getKeyspaceName();
        Token tk = mutation.key().getToken();
        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspaceName, tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspaceName);
        AbstractWriteResponseHandler<IMutation> responseHandler = rs.getWriteResponseHandler(naturalEndpoints, pendingEndpoints, consistency_level, null, writeType);
        return new WriteResponseHandlerWrapper(responseHandler, mutation);
    }

    // used by atomic_batch_mutate to decouple availability check from the write itself, caches consistency level and endpoints.
    private static class WriteResponseHandlerWrapper
    {
        final AbstractWriteResponseHandler<IMutation> handler;
        final Mutation mutation;

        WriteResponseHandlerWrapper(AbstractWriteResponseHandler<IMutation> handler, Mutation mutation)
        {
            this.handler = handler;
            this.mutation = mutation;
        }
    }

    /*
     * Replicas are picked manually:
     * - replicas should be alive according to the failure detector
     * - replicas should be in the local datacenter
     * - choose min(2, number of qualifying candiates above)
     * - allow the local node to be the only replica only if it's a single-node DC
     */
    private static Collection<InetAddress> getBatchlogEndpoints(String localDataCenter, ConsistencyLevel consistencyLevel)
    throws UnavailableException
    {
        TokenMetadata.Topology topology = StorageService.instance.getTokenMetadata().cachedOnlyTokenMap().getTopology();
        Multimap<String, InetAddress> localEndpoints = HashMultimap.create(topology.getDatacenterRacks().get(localDataCenter));
        String localRack = DatabaseDescriptor.getEndpointSnitch().getRack(FBUtilities.getBroadcastAddress());

        Collection<InetAddress> chosenEndpoints = new BatchlogManager.EndpointFilter(localRack, localEndpoints).filter();
        if (chosenEndpoints.isEmpty())
        {
            if (consistencyLevel == ConsistencyLevel.ANY)
                return Collections.singleton(FBUtilities.getBroadcastAddress());

            throw new UnavailableException(ConsistencyLevel.ONE, 1, 0);
        }

        return chosenEndpoints;
    }

    /**
     * Send the mutations to the right targets, write it locally if it corresponds or writes a hint when the node
     * is not available.
     *
     * Note about hints:
     * <pre>
     * {@code
     * | Hinted Handoff | Consist. Level |
     * | on             |       >=1      | --> wait for hints. We DO NOT notify the handler with handler.response() for hints;
     * | on             |       ANY      | --> wait for hints. Responses count towards consistency.
     * | off            |       >=1      | --> DO NOT fire hints. And DO NOT wait for them to complete.
     * | off            |       ANY      | --> DO NOT fire hints. And DO NOT wait for them to complete.
     * }
     * </pre>
     * 
     * @throws OverloadedException if the hints cannot be written/enqueued
     */
    public static void sendToHintedEndpoints(final Mutation mutation,
                                             Iterable<InetAddress> targets,
                                             AbstractWriteResponseHandler<IMutation> responseHandler,
                                             String localDataCenter)
    throws OverloadedException
    {
        // extra-datacenter replicas, grouped by dc
        Map<String, Collection<InetAddress>> dcGroups = null;
        // only need to create a Message for non-local writes
        MessageOut<Mutation> message = null;

        boolean insertLocal = false;


        for (InetAddress destination : targets)
        {
            // avoid OOMing due to excess hints.  we need to do this check even for "live" nodes, since we can
            // still generate hints for those if it's overloaded or simply dead but not yet known-to-be-dead.
            // The idea is that if we have over maxHintsInProgress hints in flight, this is probably due to
            // a small number of nodes causing problems, so we should avoid shutting down writes completely to
            // healthy nodes.  Any node with no hintsInProgress is considered healthy.
            if (StorageMetrics.totalHintsInProgress.getCount() > maxHintsInProgress
                    && (getHintsInProgressFor(destination).get() > 0 && shouldHint(destination)))
            {
                throw new OverloadedException("Too many in flight hints: " + StorageMetrics.totalHintsInProgress.getCount());
            }

            if (FailureDetector.instance.isAlive(destination))
            {
                if (canDoLocalRequest(destination))
                {
                    insertLocal = true;
                } else
                {
                    // belongs on a different server
                    if (message == null)
                        message = mutation.createMessage();
                    String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(destination);
                    // direct writes to local DC or old Cassandra versions
                    // (1.1 knows how to forward old-style String message IDs; updated to int in 2.0)
                    if (localDataCenter.equals(dc))
                    {
                        MessagingService.instance().sendRR(message, destination, responseHandler, true);
                    } else
                    {
                        Collection<InetAddress> messages = (dcGroups != null) ? dcGroups.get(dc) : null;
                        if (messages == null)
                        {
                            messages = new ArrayList<InetAddress>(3); // most DCs will have <= 3 replicas
                            if (dcGroups == null)
                                dcGroups = new HashMap<String, Collection<InetAddress>>();
                            dcGroups.put(dc, messages);
                        }
                        messages.add(destination);
                    }
                }
            } else
            {
                if (!shouldHint(destination))
                    continue;

                // Schedule a local hint
                submitHint(mutation, destination, responseHandler);
            }
        }

        if (insertLocal)
            insertLocal(mutation, responseHandler);

        if (dcGroups != null)
        {
            // for each datacenter, send the message to one node to relay the write to other replicas
            if (message == null)
                message = mutation.createMessage();

            for (Collection<InetAddress> dcTargets : dcGroups.values())
                sendMessagesToNonlocalDC(message, dcTargets, responseHandler);
        }
    }

    private static AtomicInteger getHintsInProgressFor(InetAddress destination)
    {
        try
        {
            return hintsInProgress.load(destination);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    public static Future<Void> submitHint(final Mutation mutation,
                                          final InetAddress target,
                                          final AbstractWriteResponseHandler<IMutation> responseHandler)
    {
        // local write that time out should be handled by LocalMutationRunnable
        assert !target.equals(FBUtilities.getBroadcastAddress()) : target;

        HintRunnable runnable = new HintRunnable(target)
        {
            public void runMayThrow()
            {
                int ttl = HintedHandOffManager.calculateHintTTL(mutation);
                if (ttl > 0)
                {
                    logger.debug("Adding hint for {}", target);
                    writeHintForMutation(mutation, System.currentTimeMillis(), ttl, target);
                    // Notify the handler only for CL == ANY
                    if (responseHandler != null && responseHandler.consistencyLevel == ConsistencyLevel.ANY)
                        responseHandler.response(null);
                } else
                {
                    logger.debug("Skipped writing hint for {} (ttl {})", target, ttl);
                }
            }
        };

        return submitHint(runnable);
    }

    private static Future<Void> submitHint(HintRunnable runnable)
    {
        StorageMetrics.totalHintsInProgress.inc();
        getHintsInProgressFor(runnable.target).incrementAndGet();
        return (Future<Void>) StageManager.getStage(Stage.MUTATION).submit(runnable);
    }

    /**
     * @param now current time in milliseconds - relevant for hint replay handling of truncated CFs
     */
    public static void writeHintForMutation(Mutation mutation, long now, int ttl, InetAddress target)
    {
        assert ttl > 0;
        UUID hostId = StorageService.instance.getTokenMetadata().getHostId(target);
        assert hostId != null : "Missing host ID for " + target.getHostAddress();
        HintedHandOffManager.instance.hintFor(mutation, now, ttl, hostId).apply();
        StorageMetrics.totalHints.inc();
    }

    private static void sendMessagesToNonlocalDC(MessageOut<? extends IMutation> message,
                                                 Collection<InetAddress> targets,
                                                 AbstractWriteResponseHandler<IMutation> handler)
    {
        Iterator<InetAddress> iter = targets.iterator();
        InetAddress target = iter.next();

        // Add the other destinations of the same message as a FORWARD_HEADER entry
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeInt(targets.size() - 1);
            while (iter.hasNext())
            {
                InetAddress destination = iter.next();
                CompactEndpointSerializationHelper.serialize(destination, out);
                int id = MessagingService.instance().addCallback(handler,
                                                                 message,
                                                                 destination,
                                                                 message.getTimeout(),
                                                                 handler.consistencyLevel,
                                                                 true);
                out.writeInt(id);
                logger.trace("Adding FWD message to {}@{}", id, destination);
            }
            message = message.withParameter(Mutation.FORWARD_TO, out.getData());
            // send the combined message + forward headers
            int id = MessagingService.instance().sendRR(message, target, handler, true);
            logger.trace("Sending message to {}@{}", id, target);
        }
        catch (IOException e)
        {
            // DataOutputBuffer is in-memory, doesn't throw IOException
            throw new AssertionError(e);
        }
    }

    private static void insertLocal(final Mutation mutation, final AbstractWriteResponseHandler<IMutation> responseHandler)
    {

        StageManager.getStage(Stage.MUTATION).maybeExecuteImmediately(new LocalMutationRunnable()
        {
            public void runMayThrow()
            {
                try
                {
                    mutation.apply();
                    responseHandler.response(null);
                }
                catch (Exception ex)
                {
                    logger.error("Failed to apply mutation locally : {}", ex);
                    responseHandler.onFailure(FBUtilities.getBroadcastAddress());
                }
            }
        });
    }

    /**
     * Handle counter mutation on the coordinator host.
     *
     * A counter mutation needs to first be applied to a replica (that we'll call the leader for the mutation) before being
     * replicated to the other endpoint. To achieve so, there is two case:
     *   1) the coordinator host is a replica: we proceed to applying the update locally and replicate throug
     *   applyCounterMutationOnCoordinator
     *   2) the coordinator is not a replica: we forward the (counter)mutation to a chosen replica (that will proceed through
     *   applyCounterMutationOnLeader upon receive) and wait for its acknowledgment.
     *
     * Implementation note: We check if we can fulfill the CL on the coordinator host even if he is not a replica to allow
     * quicker response and because the WriteResponseHandlers don't make it easy to send back an error. We also always gather
     * the write latencies at the coordinator node to make gathering point similar to the case of standard writes.
     */
    public static AbstractWriteResponseHandler<IMutation> mutateCounter(CounterMutation cm, String localDataCenter) throws UnavailableException, OverloadedException
    {
        InetAddress endpoint = findSuitableEndpoint(cm.getKeyspaceName(), cm.key(), localDataCenter, cm.consistency());

        if (endpoint.equals(FBUtilities.getBroadcastAddress()))
        {
            return applyCounterMutationOnCoordinator(cm, localDataCenter);
        }
        else
        {
            // Exit now if we can't fulfill the CL here instead of forwarding to the leader replica
            String keyspaceName = cm.getKeyspaceName();
            AbstractReplicationStrategy rs = Keyspace.open(keyspaceName).getReplicationStrategy();
            Token tk = cm.key().getToken();
            List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspaceName, tk);
            Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspaceName);

            rs.getWriteResponseHandler(naturalEndpoints, pendingEndpoints, cm.consistency(), null, WriteType.COUNTER).assureSufficientLiveNodes();

            // Forward the actual update to the chosen leader replica
            AbstractWriteResponseHandler<IMutation> responseHandler = new WriteResponseHandler<>(endpoint, WriteType.COUNTER);

            Tracing.trace("Enqueuing counter update to {}", endpoint);
            MessagingService.instance().sendRR(cm.makeMutationMessage(), endpoint, responseHandler, false);
            return responseHandler;
        }
    }

    /**
     * Find a suitable replica as leader for counter update.
     * For now, we pick a random replica in the local DC (or ask the snitch if
     * there is no replica alive in the local DC).
     * TODO: if we track the latency of the counter writes (which makes sense
     * contrarily to standard writes since there is a read involved), we could
     * trust the dynamic snitch entirely, which may be a better solution. It
     * is unclear we want to mix those latencies with read latencies, so this
     * may be a bit involved.
     */
    private static InetAddress findSuitableEndpoint(String keyspaceName, DecoratedKey key, String localDataCenter, ConsistencyLevel cl) throws UnavailableException
    {
        Keyspace keyspace = Keyspace.open(keyspaceName);
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        List<InetAddress> endpoints = StorageService.instance.getLiveNaturalEndpoints(keyspace, key);
        if (endpoints.isEmpty())
            // TODO have a way to compute the consistency level
            throw new UnavailableException(cl, cl.blockFor(keyspace), 0);

        List<InetAddress> localEndpoints = new ArrayList<InetAddress>();
        for (InetAddress endpoint : endpoints)
        {
            if (snitch.getDatacenter(endpoint).equals(localDataCenter))
                localEndpoints.add(endpoint);
        }
        if (localEndpoints.isEmpty())
        {
            // No endpoint in local DC, pick the closest endpoint according to the snitch
            snitch.sortByProximity(FBUtilities.getBroadcastAddress(), endpoints);
            return endpoints.get(0);
        }
        else
        {
            return localEndpoints.get(ThreadLocalRandom.current().nextInt(localEndpoints.size()));
        }
    }

    // Must be called on a replica of the mutation. This replica becomes the
    // leader of this mutation.
    public static AbstractWriteResponseHandler<IMutation> applyCounterMutationOnLeader(CounterMutation cm, String localDataCenter, Runnable callback)
    throws UnavailableException, OverloadedException
    {
        return performWrite(cm, cm.consistency(), localDataCenter, counterWritePerformer, callback, WriteType.COUNTER);
    }

    // Same as applyCounterMutationOnLeader but must with the difference that it use the MUTATION stage to execute the write (while
    // applyCounterMutationOnLeader assumes it is on the MUTATION stage already)
    public static AbstractWriteResponseHandler<IMutation> applyCounterMutationOnCoordinator(CounterMutation cm, String localDataCenter)
    throws UnavailableException, OverloadedException
    {
        return performWrite(cm, cm.consistency(), localDataCenter, counterWriteOnCoordinatorPerformer, null, WriteType.COUNTER);
    }

    private static Runnable counterWriteTask(final IMutation mutation,
                                             final Iterable<InetAddress> targets,
                                             final AbstractWriteResponseHandler<IMutation> responseHandler,
                                             final String localDataCenter)
    {
        return new DroppableRunnable(MessagingService.Verb.COUNTER_MUTATION)
        {
            @Override
            public void runMayThrow() throws OverloadedException, WriteTimeoutException
            {
                assert mutation instanceof CounterMutation;

                Mutation result = ((CounterMutation) mutation).apply();
                responseHandler.response(null);

                Set<InetAddress> remotes = Sets.difference(ImmutableSet.copyOf(targets),
                                                           ImmutableSet.of(FBUtilities.getBroadcastAddress()));
                if (!remotes.isEmpty())
                    sendToHintedEndpoints(result, remotes, responseHandler, localDataCenter);
            }
        };
    }

    private static boolean systemKeyspaceQuery(List<? extends ReadCommand> cmds)
    {
        for (ReadCommand cmd : cmds)
            if (!cmd.metadata().ksName.equals(SystemKeyspace.NAME))
                return false;
        return true;
    }

    public static RowIterator readOne(SinglePartitionReadCommand command, ConsistencyLevel consistencyLevel)
    throws UnavailableException, IsBootstrappingException, ReadFailureException, ReadTimeoutException, InvalidRequestException
    {
        return readOne(command, consistencyLevel, null);
    }

    public static RowIterator readOne(SinglePartitionReadCommand command, ConsistencyLevel consistencyLevel, ClientState state)
    throws UnavailableException, IsBootstrappingException, ReadFailureException, ReadTimeoutException, InvalidRequestException
    {
        return PartitionIterators.getOnlyElement(read(SinglePartitionReadCommand.Group.one(command), consistencyLevel, state), command);
    }

    public static PartitionIterator read(SinglePartitionReadCommand.Group group, ConsistencyLevel consistencyLevel)
    throws UnavailableException, IsBootstrappingException, ReadFailureException, ReadTimeoutException, InvalidRequestException
    {
        // When using serial CL, the ClientState should be provided
        assert !consistencyLevel.isSerialConsistency();
        return read(group, consistencyLevel, null);
    }

    /**
     * Performs the actual reading of a row out of the StorageService, fetching
     * a specific set of column names from a given column family.
     */
    public static PartitionIterator read(SinglePartitionReadCommand.Group group, ConsistencyLevel consistencyLevel, ClientState state)
    throws UnavailableException, IsBootstrappingException, ReadFailureException, ReadTimeoutException, InvalidRequestException
    {
        if (StorageService.instance.isBootstrapMode() && !systemKeyspaceQuery(group.commands))
        {
            readMetrics.unavailables.mark();
            throw new IsBootstrappingException();
        }

        return consistencyLevel.isSerialConsistency()
             ? readWithPaxos(group, consistencyLevel, state)
             : readRegular(group, consistencyLevel);
    }

    private static PartitionIterator readWithPaxos(SinglePartitionReadCommand.Group group, ConsistencyLevel consistencyLevel, ClientState state)
    throws InvalidRequestException, UnavailableException, ReadFailureException, ReadTimeoutException
    {
        assert state != null;
        if (group.commands.size() > 1)
            throw new InvalidRequestException("SERIAL/LOCAL_SERIAL consistency may only be requested for one partition at a time");

        long start = System.nanoTime();
        SinglePartitionReadCommand command = group.commands.get(0);
        CFMetaData metadata = command.metadata();
        DecoratedKey key = command.partitionKey();

        PartitionIterator result = null;
        try
        {
            // make sure any in-progress paxos writes are done (i.e., committed to a majority of replicas), before performing a quorum read
            Pair<List<InetAddress>, Integer> p = getPaxosParticipants(metadata, key, consistencyLevel);
            List<InetAddress> liveEndpoints = p.left;
            int requiredParticipants = p.right;

            // does the work of applying in-progress writes; throws UAE or timeout if it can't
            final ConsistencyLevel consistencyForCommitOrFetch = consistencyLevel == ConsistencyLevel.LOCAL_SERIAL
                                                                                   ? ConsistencyLevel.LOCAL_QUORUM
                                                                                   : ConsistencyLevel.QUORUM;

            try
            {
                final Pair<UUID, Integer> pair = beginAndRepairPaxos(start, key, metadata, liveEndpoints, requiredParticipants, consistencyLevel, consistencyForCommitOrFetch, false, state);
                if (pair.right > 0)
                    casReadMetrics.contention.update(pair.right);
            }
            catch (WriteTimeoutException e)
            {
                throw new ReadTimeoutException(consistencyLevel, 0, consistencyLevel.blockFor(Keyspace.open(metadata.ksName)), false);
            }
            catch (WriteFailureException e)
            {
                throw new ReadFailureException(consistencyLevel, e.received, e.failures, e.blockFor, false);
            }

            result = fetchRows(group.commands, consistencyForCommitOrFetch);
        }
        catch (UnavailableException e)
        {
            readMetrics.unavailables.mark();
            casReadMetrics.unavailables.mark();
            throw e;
        }
        catch (ReadTimeoutException e)
        {
            readMetrics.timeouts.mark();
            casReadMetrics.timeouts.mark();
            throw e;
        }
        catch (ReadFailureException e)
        {
            readMetrics.failures.mark();
            casReadMetrics.failures.mark();
            throw e;
        }
        finally
        {
            long latency = System.nanoTime() - start;
            readMetrics.addNano(latency);
            casReadMetrics.addNano(latency);
            Keyspace.open(metadata.ksName).getColumnFamilyStore(metadata.cfName).metric.coordinatorReadLatency.update(latency, TimeUnit.NANOSECONDS);
        }

        return result;
    }

    @SuppressWarnings("resource")
    private static PartitionIterator readRegular(SinglePartitionReadCommand.Group group, ConsistencyLevel consistencyLevel)
    throws UnavailableException, ReadFailureException, ReadTimeoutException
    {
        long start = System.nanoTime();
        try
        {
            PartitionIterator result = fetchRows(group.commands, consistencyLevel);
            // If we have more than one command, then despite each read command honoring the limit, the total result
            // might not honor it and so we should enforce it
            if (group.commands.size() > 1)
                result = group.limits().filter(result, group.nowInSec());
            return result;
        }
        catch (UnavailableException e)
        {
            readMetrics.unavailables.mark();
            throw e;
        }
        catch (ReadTimeoutException e)
        {
            readMetrics.timeouts.mark();
            throw e;
        }
        catch (ReadFailureException e)
        {
            readMetrics.failures.mark();
            throw e;
        }
        finally
        {
            long latency = System.nanoTime() - start;
            readMetrics.addNano(latency);
            // TODO avoid giving every command the same latency number.  Can fix this in CASSADRA-5329
            for (ReadCommand command : group.commands)
                Keyspace.openAndGetStore(command.metadata()).metric.coordinatorReadLatency.update(latency, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * This function executes local and remote reads, and blocks for the results:
     *
     * 1. Get the replica locations, sorted by response time according to the snitch
     * 2. Send a data request to the closest replica, and digest requests to either
     *    a) all the replicas, if read repair is enabled
     *    b) the closest R-1 replicas, where R is the number required to satisfy the ConsistencyLevel
     * 3. Wait for a response from R replicas
     * 4. If the digests (if any) match the data return the data
     * 5. else carry out read repair by getting data from all the nodes.
     */
    private static PartitionIterator fetchRows(List<SinglePartitionReadCommand<?>> commands, ConsistencyLevel consistencyLevel)
    throws UnavailableException, ReadFailureException, ReadTimeoutException
    {
        int cmdCount = commands.size();

        SinglePartitionReadLifecycle[] reads = new SinglePartitionReadLifecycle[cmdCount];
        for (int i = 0; i < cmdCount; i++)
            reads[i] = new SinglePartitionReadLifecycle(commands.get(i), consistencyLevel);

        for (int i = 0; i < cmdCount; i++)
            reads[i].doInitialQueries();

        for (int i = 0; i < cmdCount; i++)
            reads[i].maybeTryAdditionalReplicas();

        for (int i = 0; i < cmdCount; i++)
            reads[i].awaitResultsAndRetryOnDigestMismatch();

        for (int i = 0; i < cmdCount; i++)
            if (!reads[i].isDone())
                reads[i].maybeAwaitFullDataRead();

        List<PartitionIterator> results = new ArrayList<>(cmdCount);
        for (int i = 0; i < cmdCount; i++)
        {
            assert reads[i].isDone();
            results.add(reads[i].getResult());
        }

        return PartitionIterators.concat(results);
    }

    private static class SinglePartitionReadLifecycle
    {
        private final SinglePartitionReadCommand<?> command;
        private final AbstractReadExecutor executor;
        private final ConsistencyLevel consistency;

        private PartitionIterator result;
        private ReadCallback repairHandler;

        SinglePartitionReadLifecycle(SinglePartitionReadCommand<?> command, ConsistencyLevel consistency)
        {
            this.command = command;
            this.executor = AbstractReadExecutor.getReadExecutor(command, consistency);
            this.consistency = consistency;
        }

        boolean isDone()
        {
            return result != null;
        }

        void doInitialQueries()
        {
            executor.executeAsync();
        }

        void maybeTryAdditionalReplicas()
        {
            executor.maybeTryAdditionalReplicas();
        }

        void awaitResultsAndRetryOnDigestMismatch() throws ReadFailureException, ReadTimeoutException
        {
            try
            {
                result = executor.get();
            }
            catch (DigestMismatchException ex)
            {
                Tracing.trace("Digest mismatch: {}", ex);

                ReadRepairMetrics.repairedBlocking.mark();

                // Do a full data read to resolve the correct response (and repair node that need be)
                Keyspace keyspace = Keyspace.open(command.metadata().ksName);
                DataResolver resolver = new DataResolver(keyspace, command, ConsistencyLevel.ALL, executor.handler.endpoints.size());
                repairHandler = new ReadCallback(resolver,
                                                 ConsistencyLevel.ALL,
                                                 executor.getContactedReplicas().size(),
                                                 command,
                                                 keyspace,
                                                 executor.handler.endpoints);

                MessageOut<ReadCommand> message = command.createMessage();
                for (InetAddress endpoint : executor.getContactedReplicas())
                {
                    Tracing.trace("Enqueuing full data read to {}", endpoint);
                    MessagingService.instance().sendRRWithFailure(message, endpoint, repairHandler);
                }
            }
        }

        void maybeAwaitFullDataRead() throws ReadTimeoutException
        {
            // There wasn't a digest mismatch, we're good
            if (repairHandler == null)
                return;

            // Otherwise, get the result from the full-data read and check that it's not a short read
            try
            {
                result = repairHandler.get();
            }
            catch (DigestMismatchException e)
            {
                throw new AssertionError(e); // full data requested from each node here, no digests should be sent
            }
            catch (ReadTimeoutException e)
            {
                if (Tracing.isTracing())
                    Tracing.trace("Timed out waiting on digest mismatch repair requests");
                else
                    logger.debug("Timed out waiting on digest mismatch repair requests");
                // the caught exception here will have CL.ALL from the repair command,
                // not whatever CL the initial command was at (CASSANDRA-7947)
                int blockFor = consistency.blockFor(Keyspace.open(command.metadata().ksName));
                throw new ReadTimeoutException(consistency, blockFor-1, blockFor, true);
            }
        }

        PartitionIterator getResult()
        {
            assert result != null;
            return result;
        }
    }

    static class LocalReadRunnable extends DroppableRunnable
    {
        private final ReadCommand command;
        private final ReadCallback handler;
        private final long start = System.nanoTime();

        LocalReadRunnable(ReadCommand command, ReadCallback handler)
        {
            super(MessagingService.Verb.READ);
            this.command = command;
            this.handler = handler;
        }

        protected void runMayThrow()
        {
            try
            {
                try (ReadOrderGroup orderGroup = command.startOrderGroup(); UnfilteredPartitionIterator iterator = command.executeLocally(orderGroup))
                {
                    handler.response(command.createResponse(iterator));
                }
                MessagingService.instance().addLatency(FBUtilities.getBroadcastAddress(), TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            }
            catch (Throwable t)
            {
                handler.onFailure(FBUtilities.getBroadcastAddress());
                if (t instanceof TombstoneOverwhelmingException)
                    logger.error(t.getMessage());
                else
                    throw t;
            }
        }
    }

    public static List<InetAddress> getLiveSortedEndpoints(Keyspace keyspace, ByteBuffer key)
    {
        return getLiveSortedEndpoints(keyspace, StorageService.getPartitioner().decorateKey(key));
    }

    public static List<InetAddress> getLiveSortedEndpoints(Keyspace keyspace, RingPosition pos)
    {
        List<InetAddress> liveEndpoints = StorageService.instance.getLiveNaturalEndpoints(keyspace, pos);
        DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getBroadcastAddress(), liveEndpoints);
        return liveEndpoints;
    }

    private static List<InetAddress> intersection(List<InetAddress> l1, List<InetAddress> l2)
    {
        // Note: we don't use Guava Sets.intersection() for 3 reasons:
        //   1) retainAll would be inefficient if l1 and l2 are large but in practice both are the replicas for a range and
        //   so will be very small (< RF). In that case, retainAll is in fact more efficient.
        //   2) we do ultimately need a list so converting everything to sets don't make sense
        //   3) l1 and l2 are sorted by proximity. The use of retainAll  maintain that sorting in the result, while using sets wouldn't.
        List<InetAddress> inter = new ArrayList<InetAddress>(l1);
        inter.retainAll(l2);
        return inter;
    }

    /**
     * Estimate the number of result rows (either cql3 rows or "thrift" rows, as called for by the command) per
     * range in the ring based on our local data.  This assumes that ranges are uniformly distributed across the cluster
     * and that the queried data is also uniformly distributed.
     */
    private static float estimateResultsPerRange(PartitionRangeReadCommand command, Keyspace keyspace)
    {
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.metadata().cfId);
        SecondaryIndexSearcher searcher = cfs.indexManager.getBestIndexSearcherFor(command);

        float maxExpectedResults = searcher == null
                                 ? command.limits().estimateTotalResults(cfs)
                                 : searcher.highestSelectivityIndex(command.rowFilter()).estimateResultRows();

        // adjust maxExpectedResults by the number of tokens this node has and the replication factor for this ks
        return (maxExpectedResults / DatabaseDescriptor.getNumTokens()) / keyspace.getReplicationStrategy().getReplicationFactor();
    }

    private static class RangeForQuery
    {
        public final AbstractBounds<PartitionPosition> range;
        public final List<InetAddress> liveEndpoints;
        public final List<InetAddress> filteredEndpoints;

        public RangeForQuery(AbstractBounds<PartitionPosition> range, List<InetAddress> liveEndpoints, List<InetAddress> filteredEndpoints)
        {
            this.range = range;
            this.liveEndpoints = liveEndpoints;
            this.filteredEndpoints = filteredEndpoints;
        }
    }

    private static class RangeIterator extends AbstractIterator<RangeForQuery>
    {
        private final Keyspace keyspace;
        private final ConsistencyLevel consistency;
        private final Iterator<? extends AbstractBounds<PartitionPosition>> ranges;
        private final int rangeCount;

        public RangeIterator(PartitionRangeReadCommand command, Keyspace keyspace, ConsistencyLevel consistency)
        {
            this.keyspace = keyspace;
            this.consistency = consistency;

            List<? extends AbstractBounds<PartitionPosition>> l = keyspace.getReplicationStrategy() instanceof LocalStrategy
                                                          ? command.dataRange().keyRange().unwrap()
                                                          : getRestrictedRanges(command.dataRange().keyRange());
            this.ranges = l.iterator();
            this.rangeCount = l.size();
        }

        public int rangeCount()
        {
            return rangeCount;
        }

        protected RangeForQuery computeNext()
        {
            if (!ranges.hasNext())
                return endOfData();

            AbstractBounds<PartitionPosition> range = ranges.next();
            List<InetAddress> liveEndpoints = getLiveSortedEndpoints(keyspace, range.right);
            return new RangeForQuery(range,
                                     liveEndpoints,
                                     consistency.filterForQuery(keyspace, liveEndpoints));
        }
    }

    private static class RangeMerger extends AbstractIterator<RangeForQuery>
    {
        private final Keyspace keyspace;
        private final ConsistencyLevel consistency;
        private final PeekingIterator<RangeForQuery> ranges;

        private RangeMerger(Iterator<RangeForQuery> iterator, Keyspace keyspace, ConsistencyLevel consistency)
        {
            this.keyspace = keyspace;
            this.consistency = consistency;
            this.ranges = Iterators.peekingIterator(iterator);
        }

        protected RangeForQuery computeNext()
        {
            if (!ranges.hasNext())
                return endOfData();

            RangeForQuery current = ranges.next();

            // getRestrictedRange has broken the queried range into per-[vnode] token ranges, but this doesn't take
            // the replication factor into account. If the intersection of live endpoints for 2 consecutive ranges
            // still meets the CL requirements, then we can merge both ranges into the same RangeSliceCommand.
            while (ranges.hasNext())
            {
                // If the current range right is the min token, we should stop merging because CFS.getRangeSlice
                // don't know how to deal with a wrapping range.
                // Note: it would be slightly more efficient to have CFS.getRangeSlice on the destination nodes unwraps
                // the range if necessary and deal with it. However, we can't start sending wrapped range without breaking
                // wire compatibility, so It's likely easier not to bother;
                if (current.range.right.isMinimum())
                    break;

                RangeForQuery next = ranges.peek();

                List<InetAddress> merged = intersection(current.liveEndpoints, next.liveEndpoints);

                // Check if there is enough endpoint for the merge to be possible.
                if (!consistency.isSufficientLiveNodes(keyspace, merged))
                    break;

                List<InetAddress> filteredMerged = consistency.filterForQuery(keyspace, merged);

                // Estimate whether merging will be a win or not
                if (!DatabaseDescriptor.getEndpointSnitch().isWorthMergingForRangeQuery(filteredMerged, current.filteredEndpoints, next.filteredEndpoints))
                    break;

                // If we get there, merge this range and the next one
                current = new RangeForQuery(current.range.withNewRight(next.range.right), merged, filteredMerged);
                ranges.next(); // consume the range we just merged since we've only peeked so far
            }
            return current;
        }
    }

    private static class SingleRangeResponse extends AbstractIterator<RowIterator> implements PartitionIterator
    {
        private final ReadCallback handler;
        private PartitionIterator result;

        private SingleRangeResponse(ReadCallback handler)
        {
            this.handler = handler;
        }

        private void waitForResponse() throws ReadTimeoutException
        {
            if (result != null)
                return;

            try
            {
                result = handler.get();
            }
            catch (DigestMismatchException e)
            {
                throw new AssertionError(e); // no digests in range slices yet
            }
        }

        protected RowIterator computeNext()
        {
            waitForResponse();
            return result.hasNext() ? result.next() : endOfData();
        }

        public void close()
        {
            if (result != null)
                result.close();
        }
    }

    private static class RangeCommandIterator extends AbstractIterator<RowIterator> implements PartitionIterator
    {
        private final Iterator<RangeForQuery> ranges;
        private final int totalRangeCount;
        private final PartitionRangeReadCommand command;
        private final Keyspace keyspace;
        private final ConsistencyLevel consistency;

        private final long startTime;
        private CountingPartitionIterator sentQueryIterator;

        private int concurrencyFactor;
        // The two following "metric" are maintained to improve the concurrencyFactor
        // when it was not good enough initially.
        private int liveReturned;
        private int rangesQueried;

        public RangeCommandIterator(RangeIterator ranges, PartitionRangeReadCommand command, int concurrencyFactor, Keyspace keyspace, ConsistencyLevel consistency)
        {
            this.command = command;
            this.concurrencyFactor = concurrencyFactor;
            this.startTime = System.nanoTime();
            this.ranges = new RangeMerger(ranges, keyspace, consistency);
            this.totalRangeCount = ranges.rangeCount();
            this.consistency = consistency;
            this.keyspace = keyspace;
        }

        public RowIterator computeNext()
        {
            while (sentQueryIterator == null || !sentQueryIterator.hasNext())
            {
                // If we don't have more range to handle, we're done
                if (!ranges.hasNext())
                    return endOfData();

                // else, sends the next batch of concurrent queries (after having close the previous iterator)
                if (sentQueryIterator != null)
                {
                    liveReturned += sentQueryIterator.counter().counted();
                    sentQueryIterator.close();

                    // It's not the first batch of queries and we're not done, so we we can use what has been
                    // returned so far to improve our rows-per-range estimate and update the concurrency accordingly
                    updateConcurrencyFactor();
                }
                sentQueryIterator = sendNextRequests();
            }

            return sentQueryIterator.next();
        }

        private void updateConcurrencyFactor()
        {
            if (liveReturned == 0)
            {
                // we haven't actually gotten any results, so query all remaining ranges at once
                concurrencyFactor = totalRangeCount - rangesQueried;
                return;
            }

            // Otherwise, compute how many rows per range we got on average and pick a concurrency factor
            // that should allow us to fetch all remaining rows with the next batch of (concurrent) queries.
            int remainingRows = command.limits().count() - liveReturned;
            float rowsPerRange = (float)liveReturned / (float)rangesQueried;
            concurrencyFactor = Math.max(1, Math.min(totalRangeCount - rangesQueried, Math.round(remainingRows / rowsPerRange)));
            logger.debug("Didn't get enough response rows; actual rows per range: {}; remaining rows: {}, new concurrent requests: {}",
                         rowsPerRange, (int) remainingRows, concurrencyFactor);
        }

        private SingleRangeResponse query(RangeForQuery toQuery)
        {
            PartitionRangeReadCommand rangeCommand = command.forSubRange(toQuery.range);

            DataResolver resolver = new DataResolver(keyspace, rangeCommand, consistency, toQuery.filteredEndpoints.size());

            int blockFor = consistency.blockFor(keyspace);
            int minResponses = Math.min(toQuery.filteredEndpoints.size(), blockFor);
            List<InetAddress> minimalEndpoints = toQuery.filteredEndpoints.subList(0, minResponses);
            ReadCallback handler = new ReadCallback(resolver, consistency, rangeCommand, minimalEndpoints);

            handler.assureSufficientLiveNodes();

            if (toQuery.filteredEndpoints.size() == 1 && canDoLocalRequest(toQuery.filteredEndpoints.get(0)))
            {
                StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(rangeCommand, handler), Tracing.instance.get());
            }
            else
            {
                MessageOut<ReadCommand> message = rangeCommand.createMessage();
                for (InetAddress endpoint : toQuery.filteredEndpoints)
                {
                    Tracing.trace("Enqueuing request to {}", endpoint);
                    MessagingService.instance().sendRRWithFailure(message, endpoint, handler);
                }
            }

            return new SingleRangeResponse(handler);
        }

        private CountingPartitionIterator sendNextRequests()
        {
            List<PartitionIterator> concurrentQueries = new ArrayList<>(concurrencyFactor);
            for (int i = 0; i < concurrencyFactor && ranges.hasNext(); i++)
            {
                concurrentQueries.add(query(ranges.next()));
                ++rangesQueried;
            }

            Tracing.trace("Submitted {} concurrent range requests", concurrentQueries.size());
            return new CountingPartitionIterator(PartitionIterators.concat(concurrentQueries), command.limits(), command.nowInSec());
        }

        public void close()
        {
            try
            {
                if (sentQueryIterator != null)
                    sentQueryIterator.close();
            }
            finally
            {
                long latency = System.nanoTime() - startTime;
                rangeMetrics.addNano(latency);
                Keyspace.openAndGetStore(command.metadata()).metric.coordinatorScanLatency.update(latency, TimeUnit.NANOSECONDS);
            }
        }
    }

    @SuppressWarnings("resource")
    public static PartitionIterator getRangeSlice(PartitionRangeReadCommand command, ConsistencyLevel consistencyLevel)
    throws UnavailableException, ReadFailureException, ReadTimeoutException
    {
        Tracing.trace("Computing ranges to query");

        Keyspace keyspace = Keyspace.open(command.metadata().ksName);
        RangeIterator ranges = new RangeIterator(command, keyspace, consistencyLevel);

        // our estimate of how many result rows there will be per-range
        float resultsPerRange = estimateResultsPerRange(command, keyspace);
        // underestimate how many rows we will get per-range in order to increase the likelihood that we'll
        // fetch enough rows in the first round
        resultsPerRange -= resultsPerRange * CONCURRENT_SUBREQUESTS_MARGIN;
        int concurrencyFactor = resultsPerRange == 0.0
                              ? 1
                              : Math.max(1, Math.min(ranges.rangeCount(), (int) Math.ceil(command.limits().count() / resultsPerRange)));
        logger.debug("Estimated result rows per range: {}; requested rows: {}, ranges.size(): {}; concurrent range requests: {}",
                     resultsPerRange, command.limits().count(), ranges.rangeCount(), concurrencyFactor);
        Tracing.trace("Submitting range requests on {} ranges with a concurrency of {} ({} rows per range expected)", ranges.rangeCount(), concurrencyFactor, resultsPerRange);

        // Note that in general, a RangeCommandIterator will honor the command limit for each range, but will not enforce it globally.
        return command.postReconciliationProcessing(command.limits().filter(new RangeCommandIterator(ranges, command, concurrencyFactor, keyspace, consistencyLevel), command.nowInSec()));
    }

    public Map<String, List<String>> getSchemaVersions()
    {
        return describeSchemaVersions();
    }

    /**
     * initiate a request/response session with each live node to check whether or not everybody is using the same
     * migration id. This is useful for determining if a schema change has propagated through the cluster. Disagreement
     * is assumed if any node fails to respond.
     */
    public static Map<String, List<String>> describeSchemaVersions()
    {
        final String myVersion = Schema.instance.getVersion().toString();
        final Map<InetAddress, UUID> versions = new ConcurrentHashMap<InetAddress, UUID>();
        final Set<InetAddress> liveHosts = Gossiper.instance.getLiveMembers();
        final CountDownLatch latch = new CountDownLatch(liveHosts.size());

        IAsyncCallback<UUID> cb = new IAsyncCallback<UUID>()
        {
            public void response(MessageIn<UUID> message)
            {
                // record the response from the remote node.
                versions.put(message.from, message.payload);
                latch.countDown();
            }

            public boolean isLatencyForSnitch()
            {
                return false;
            }
        };
        // an empty message acts as a request to the SchemaCheckVerbHandler.
        MessageOut message = new MessageOut(MessagingService.Verb.SCHEMA_CHECK);
        for (InetAddress endpoint : liveHosts)
            MessagingService.instance().sendRR(message, endpoint, cb);

        try
        {
            // wait for as long as possible. timeout-1s if possible.
            latch.await(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError("This latch shouldn't have been interrupted.");
        }

        // maps versions to hosts that are on that version.
        Map<String, List<String>> results = new HashMap<String, List<String>>();
        Iterable<InetAddress> allHosts = Iterables.concat(Gossiper.instance.getLiveMembers(), Gossiper.instance.getUnreachableMembers());
        for (InetAddress host : allHosts)
        {
            UUID version = versions.get(host);
            String stringVersion = version == null ? UNREACHABLE : version.toString();
            List<String> hosts = results.get(stringVersion);
            if (hosts == null)
            {
                hosts = new ArrayList<String>();
                results.put(stringVersion, hosts);
            }
            hosts.add(host.getHostAddress());
        }

        // we're done: the results map is ready to return to the client.  the rest is just debug logging:
        if (results.get(UNREACHABLE) != null)
            logger.debug("Hosts not in agreement. Didn't get a response from everybody: {}", StringUtils.join(results.get(UNREACHABLE), ","));
        for (Map.Entry<String, List<String>> entry : results.entrySet())
        {
            // check for version disagreement. log the hosts that don't agree.
            if (entry.getKey().equals(UNREACHABLE) || entry.getKey().equals(myVersion))
                continue;
            for (String host : entry.getValue())
                logger.debug("{} disagrees ({})", host, entry.getKey());
        }
        if (results.size() == 1)
            logger.debug("Schemas are in agreement.");

        return results;
    }

    /**
     * Compute all ranges we're going to query, in sorted order. Nodes can be replica destinations for many ranges,
     * so we need to restrict each scan to the specific range we want, or else we'd get duplicate results.
     */
    static <T extends RingPosition<T>> List<AbstractBounds<T>> getRestrictedRanges(final AbstractBounds<T> queryRange)
    {
        // special case for bounds containing exactly 1 (non-minimum) token
        if (queryRange instanceof Bounds && queryRange.left.equals(queryRange.right) && !queryRange.left.isMinimum())
        {
            return Collections.singletonList(queryRange);
        }

        TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();

        List<AbstractBounds<T>> ranges = new ArrayList<AbstractBounds<T>>();
        // divide the queryRange into pieces delimited by the ring and minimum tokens
        Iterator<Token> ringIter = TokenMetadata.ringIterator(tokenMetadata.sortedTokens(), queryRange.left.getToken(), true);
        AbstractBounds<T> remainder = queryRange;
        while (ringIter.hasNext())
        {
            /*
             * remainder can be a range/bounds of token _or_ keys and we want to split it with a token:
             *   - if remainder is tokens, then we'll just split using the provided token.
             *   - if remainder is keys, we want to split using token.upperBoundKey. For instance, if remainder
             *     is [DK(10, 'foo'), DK(20, 'bar')], and we have 3 nodes with tokens 0, 15, 30. We want to
             *     split remainder to A=[DK(10, 'foo'), 15] and B=(15, DK(20, 'bar')]. But since we can't mix
             *     tokens and keys at the same time in a range, we uses 15.upperBoundKey() to have A include all
             *     keys having 15 as token and B include none of those (since that is what our node owns).
             * asSplitValue() abstracts that choice.
             */
            Token upperBoundToken = ringIter.next();
            T upperBound = (T)upperBoundToken.upperBound(queryRange.left.getClass());
            if (!remainder.left.equals(upperBound) && !remainder.contains(upperBound))
                // no more splits
                break;
            Pair<AbstractBounds<T>,AbstractBounds<T>> splits = remainder.split(upperBound);
            if (splits == null)
                continue;

            ranges.add(splits.left);
            remainder = splits.right;
        }
        ranges.add(remainder);

        return ranges;
    }

    public boolean getHintedHandoffEnabled()
    {
        return DatabaseDescriptor.hintedHandoffEnabled();
    }

    public void setHintedHandoffEnabled(boolean b)
    {
        DatabaseDescriptor.setHintedHandoffEnabled(b);
    }

    public void enableHintsForDC(String dc)
    {
        DatabaseDescriptor.enableHintsForDC(dc);
    }

    public void disableHintsForDC(String dc)
    {
        DatabaseDescriptor.disableHintsForDC(dc);
    }

    public Set<String> getHintedHandoffDisabledDCs()
    {
        return DatabaseDescriptor.hintedHandoffDisabledDCs();
    }

    public int getMaxHintWindow()
    {
        return DatabaseDescriptor.getMaxHintWindow();
    }

    public void setMaxHintWindow(int ms)
    {
        DatabaseDescriptor.setMaxHintWindow(ms);
    }

    public static boolean shouldHint(InetAddress ep)
    {
        if (!DatabaseDescriptor.hintedHandoffEnabled())
        {
            HintedHandOffManager.instance.metrics.incrPastWindow(ep);
            return false;
        }

        Set<String> disabledDCs = DatabaseDescriptor.hintedHandoffDisabledDCs();
        if (!disabledDCs.isEmpty())
        {
            final String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(ep);
            if (disabledDCs.contains(dc))
            {
                Tracing.trace("Not hinting {} since its data center {} has been disabled {}", ep, dc, disabledDCs);
                HintedHandOffManager.instance.metrics.incrPastWindow(ep);
                return false;
            }
        }

        boolean hintWindowExpired = Gossiper.instance.getEndpointDowntime(ep) > DatabaseDescriptor.getMaxHintWindow();
        if (hintWindowExpired)
        {
            HintedHandOffManager.instance.metrics.incrPastWindow(ep);
            Tracing.trace("Not hinting {} which has been down {} ms", ep, Gossiper.instance.getEndpointDowntime(ep));
        }
        return !hintWindowExpired;
    }

    /**
     * Performs the truncate operatoin, which effectively deletes all data from
     * the column family cfname
     * @param keyspace
     * @param cfname
     * @throws UnavailableException If some of the hosts in the ring are down.
     * @throws TimeoutException
     * @throws IOException
     */
    public static void truncateBlocking(String keyspace, String cfname) throws UnavailableException, TimeoutException, IOException
    {
        logger.debug("Starting a blocking truncate operation on keyspace {}, CF {}", keyspace, cfname);
        if (isAnyStorageHostDown())
        {
            logger.info("Cannot perform truncate, some hosts are down");
            // Since the truncate operation is so aggressive and is typically only
            // invoked by an admin, for simplicity we require that all nodes are up
            // to perform the operation.
            int liveMembers = Gossiper.instance.getLiveMembers().size();
            throw new UnavailableException(ConsistencyLevel.ALL, liveMembers + Gossiper.instance.getUnreachableMembers().size(), liveMembers);
        }

        Set<InetAddress> allEndpoints = Gossiper.instance.getLiveTokenOwners();
        
        int blockFor = allEndpoints.size();
        final TruncateResponseHandler responseHandler = new TruncateResponseHandler(blockFor);

        // Send out the truncate calls and track the responses with the callbacks.
        Tracing.trace("Enqueuing truncate messages to hosts {}", allEndpoints);
        final Truncation truncation = new Truncation(keyspace, cfname);
        MessageOut<Truncation> message = truncation.createMessage();
        for (InetAddress endpoint : allEndpoints)
            MessagingService.instance().sendRR(message, endpoint, responseHandler);

        // Wait for all
        try
        {
            responseHandler.get();
        }
        catch (TimeoutException e)
        {
            Tracing.trace("Timed out");
            throw e;
        }
    }

    /**
     * Asks the gossiper if there are any nodes that are currently down.
     * @return true if the gossiper thinks all nodes are up.
     */
    private static boolean isAnyStorageHostDown()
    {
        return !Gossiper.instance.getUnreachableTokenOwners().isEmpty();
    }

    public interface WritePerformer
    {
        public void apply(IMutation mutation,
                          Iterable<InetAddress> targets,
                          AbstractWriteResponseHandler<IMutation> responseHandler,
                          String localDataCenter,
                          ConsistencyLevel consistencyLevel) throws OverloadedException;
    }

    /**
     * A Runnable that aborts if it doesn't start running before it times out
     */
    private static abstract class DroppableRunnable implements Runnable
    {
        private final long constructionTime = System.nanoTime();
        private final MessagingService.Verb verb;

        public DroppableRunnable(MessagingService.Verb verb)
        {
            this.verb = verb;
        }

        public final void run()
        {

            if (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - constructionTime) > DatabaseDescriptor.getTimeout(verb))
            {
                MessagingService.instance().incrementDroppedMessages(verb);
                return;
            }
            try
            {
                runMayThrow();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        abstract protected void runMayThrow() throws Exception;
    }

    /**
     * Like DroppableRunnable, but if it aborts, it will rerun (on the mutation stage) after
     * marking itself as a hint in progress so that the hint backpressure mechanism can function.
     */
    private static abstract class LocalMutationRunnable implements Runnable
    {
        private final long constructionTime = System.currentTimeMillis();

        public final void run()
        {
            if (System.currentTimeMillis() > constructionTime + DatabaseDescriptor.getTimeout(MessagingService.Verb.MUTATION))
            {
                MessagingService.instance().incrementDroppedMessages(MessagingService.Verb.MUTATION);
                HintRunnable runnable = new HintRunnable(FBUtilities.getBroadcastAddress())
                {
                    protected void runMayThrow() throws Exception
                    {
                        LocalMutationRunnable.this.runMayThrow();
                    }
                };
                submitHint(runnable);
                return;
            }

            try
            {
                runMayThrow();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        abstract protected void runMayThrow() throws Exception;
    }

    /**
     * HintRunnable will decrease totalHintsInProgress and targetHints when finished.
     * It is the caller's responsibility to increment them initially.
     */
    private abstract static class HintRunnable implements Runnable
    {
        public final InetAddress target;

        protected HintRunnable(InetAddress target)
        {
            this.target = target;
        }

        public void run()
        {
            try
            {
                runMayThrow();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                StorageMetrics.totalHintsInProgress.dec();
                getHintsInProgressFor(target).decrementAndGet();
            }
        }

        abstract protected void runMayThrow() throws Exception;
    }

    public long getTotalHints()
    {
        return StorageMetrics.totalHints.getCount();
    }

    public int getMaxHintsInProgress()
    {
        return maxHintsInProgress;
    }

    public void setMaxHintsInProgress(int qs)
    {
        maxHintsInProgress = qs;
    }

    public int getHintsInProgress()
    {
        return (int) StorageMetrics.totalHintsInProgress.getCount();
    }

    public void verifyNoHintsInProgress()
    {
        if (getHintsInProgress() > 0)
            logger.warn("Some hints were not written before shutdown.  This is not supposed to happen.  You should (a) run repair, and (b) file a bug report");
    }

    public Long getRpcTimeout() { return DatabaseDescriptor.getRpcTimeout(); }
    public void setRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setRpcTimeout(timeoutInMillis); }

    public Long getReadRpcTimeout() { return DatabaseDescriptor.getReadRpcTimeout(); }
    public void setReadRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setReadRpcTimeout(timeoutInMillis); }

    public Long getWriteRpcTimeout() { return DatabaseDescriptor.getWriteRpcTimeout(); }
    public void setWriteRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setWriteRpcTimeout(timeoutInMillis); }

    public Long getCounterWriteRpcTimeout() { return DatabaseDescriptor.getCounterWriteRpcTimeout(); }
    public void setCounterWriteRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setCounterWriteRpcTimeout(timeoutInMillis); }

    public Long getCasContentionTimeout() { return DatabaseDescriptor.getCasContentionTimeout(); }
    public void setCasContentionTimeout(Long timeoutInMillis) { DatabaseDescriptor.setCasContentionTimeout(timeoutInMillis); }

    public Long getRangeRpcTimeout() { return DatabaseDescriptor.getRangeRpcTimeout(); }
    public void setRangeRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setRangeRpcTimeout(timeoutInMillis); }

    public Long getTruncateRpcTimeout() { return DatabaseDescriptor.getTruncateRpcTimeout(); }
    public void setTruncateRpcTimeout(Long timeoutInMillis) { DatabaseDescriptor.setTruncateRpcTimeout(timeoutInMillis); }

    public Long getNativeTransportMaxConcurrentConnections() { return DatabaseDescriptor.getNativeTransportMaxConcurrentConnections(); }
    public void setNativeTransportMaxConcurrentConnections(Long nativeTransportMaxConcurrentConnections) { DatabaseDescriptor.setNativeTransportMaxConcurrentConnections(nativeTransportMaxConcurrentConnections); }

    public Long getNativeTransportMaxConcurrentConnectionsPerIp() { return DatabaseDescriptor.getNativeTransportMaxConcurrentConnectionsPerIp(); }
    public void setNativeTransportMaxConcurrentConnectionsPerIp(Long nativeTransportMaxConcurrentConnections) { DatabaseDescriptor.setNativeTransportMaxConcurrentConnectionsPerIp(nativeTransportMaxConcurrentConnections); }

    public void reloadTriggerClasses() { TriggerExecutor.instance.reloadClasses(); }

    public long getReadRepairAttempted() {
        return ReadRepairMetrics.attempted.getCount();
    }
    
    public long getReadRepairRepairedBlocking() {
        return ReadRepairMetrics.repairedBlocking.getCount();
    }
    
    public long getReadRepairRepairedBackground() {
        return ReadRepairMetrics.repairedBackground.getCount();
    }
}
