/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.proto;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ADD_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ADD_ENTRY_BLOCKED;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ADD_ENTRY_BLOCKED_WAIT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ADD_ENTRY_IN_PROGRESS;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.ADD_ENTRY_REQUEST;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.CHANNEL_WRITE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.FORCE_LEDGER;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.FORCE_LEDGER_REQUEST;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.GET_BOOKIE_INFO;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.GET_BOOKIE_INFO_REQUEST;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_BLOCKED;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_BLOCKED_WAIT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_FENCE_READ;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_FENCE_REQUEST;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_FENCE_WAIT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_IN_PROGRESS;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_LONG_POLL_PRE_WAIT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_LONG_POLL_READ;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_LONG_POLL_REQUEST;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_LONG_POLL_WAIT;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_REQUEST;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_ENTRY_SCHEDULING_DELAY;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_LAC;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_LAC_REQUEST;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.READ_LAST_ENTRY_NOENTRY_ERROR;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.WRITE_LAC;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.WRITE_LAC_REQUEST;
import static org.apache.bookkeeper.proto.RequestUtils.hasFlag;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.HashedWheelTimer;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import lombok.AccessLevel;
import lombok.Getter;
import org.apache.bookkeeper.auth.AuthProviderFactoryFactory;
import org.apache.bookkeeper.auth.AuthToken;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.processor.RequestProcessor;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.tls.SecurityHandlerFactory;
import org.apache.bookkeeper.tls.SecurityHandlerFactory.NodeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the RequestProcessor interface.
 */
@Getter(AccessLevel.PACKAGE)
public class BookieRequestProcessor implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(BookieRequestProcessor.class);

    /**
     * The server configuration. We use this for getting the number of add and read
     * worker threads.
     */
    private final ServerConfiguration serverCfg;
    private final long waitTimeoutOnBackpressureMillis;

    /**
     * This is the Bookie instance that is used to handle all read and write requests.
     */
    final Bookie bookie;

    /**
     * The threadpool used to execute all read entry requests issued to this server.
     */
    private final OrderedExecutor readThreadPool;

    /**
     * The threadpool used to execute all add entry requests issued to this server.
     */
    private final OrderedExecutor writeThreadPool;

    /**
     * TLS management.
     */
    private final SecurityHandlerFactory shFactory;

    /**
     * The threadpool used to execute all long poll requests issued to this server
     * after they are done waiting.
     */
    private final OrderedExecutor longPollThreadPool;

    /**
     * The threadpool used to execute high priority requests.
     */
    private final OrderedExecutor highPriorityThreadPool;

    /**
     * The Timer used to time out requests for long polling.
     */
    private final HashedWheelTimer requestTimer;

    // Expose Stats
    private final BKStats bkStats = BKStats.getInstance();
    private final boolean statsEnabled;
    private final OpStatsLogger addRequestStats;
    private final OpStatsLogger addEntryStats;
    final OpStatsLogger readRequestStats;
    final OpStatsLogger readEntryStats;
    final OpStatsLogger forceLedgerStats;
    final OpStatsLogger forceLedgerRequestStats;
    final OpStatsLogger fenceReadRequestStats;
    final OpStatsLogger fenceReadEntryStats;
    final OpStatsLogger fenceReadWaitStats;
    final OpStatsLogger readEntrySchedulingDelayStats;
    final OpStatsLogger longPollPreWaitStats;
    final OpStatsLogger longPollWaitStats;
    final OpStatsLogger longPollReadStats;
    final OpStatsLogger longPollReadRequestStats;
    final Counter readLastEntryNoEntryErrorCounter;
    final OpStatsLogger writeLacRequestStats;
    final OpStatsLogger writeLacStats;
    final OpStatsLogger readLacRequestStats;
    final OpStatsLogger readLacStats;
    final OpStatsLogger getBookieInfoRequestStats;
    final OpStatsLogger getBookieInfoStats;
    final OpStatsLogger channelWriteStats;
    final OpStatsLogger addEntryBlockedStats;
    final OpStatsLogger readEntryBlockedStats;

    final AtomicInteger addsInProgress = new AtomicInteger(0);
    final AtomicInteger maxAddsInProgress = new AtomicInteger(0);
    final AtomicInteger addsBlocked = new AtomicInteger(0);
    final AtomicInteger readsInProgress = new AtomicInteger(0);
    final AtomicInteger readsBlocked = new AtomicInteger(0);
    final AtomicInteger maxReadsInProgress = new AtomicInteger(0);

    final Semaphore addsSemaphore;
    final Semaphore readsSemaphore;

    // to temporary blacklist channels
    final Optional<Cache<Channel, Boolean>> blacklistedChannels;
    final Consumer<Channel> onResponseTimeout;

    public BookieRequestProcessor(ServerConfiguration serverCfg, Bookie bookie,
            StatsLogger statsLogger, SecurityHandlerFactory shFactory) throws SecurityException {
        this.serverCfg = serverCfg;
        this.waitTimeoutOnBackpressureMillis = serverCfg.getWaitTimeoutOnResponseBackpressureMillis();
        this.bookie = bookie;
        this.readThreadPool = createExecutor(
                this.serverCfg.getNumReadWorkerThreads(),
                "BookieReadThreadPool",
                serverCfg.getMaxPendingReadRequestPerThread(),
                statsLogger);
        this.writeThreadPool = createExecutor(
                this.serverCfg.getNumAddWorkerThreads(),
                "BookieWriteThreadPool",
                serverCfg.getMaxPendingAddRequestPerThread(),
                statsLogger);
        if (serverCfg.getNumLongPollWorkerThreads() <= 0 && readThreadPool != null) {
            this.longPollThreadPool = this.readThreadPool;
        } else {
            int numThreads = this.serverCfg.getNumLongPollWorkerThreads();
            if (numThreads <= 0) {
                numThreads = Runtime.getRuntime().availableProcessors();
            }
            this.longPollThreadPool = createExecutor(
                numThreads,
                "BookieLongPollThread-" + serverCfg.getBookiePort(),
                OrderedExecutor.NO_TASK_LIMIT, statsLogger);
        }
        this.highPriorityThreadPool = createExecutor(
                this.serverCfg.getNumHighPriorityWorkerThreads(),
                "BookieHighPriorityThread-" + serverCfg.getBookiePort(),
                OrderedExecutor.NO_TASK_LIMIT, statsLogger);
        this.shFactory = shFactory;
        if (shFactory != null) {
            shFactory.init(NodeType.Server, serverCfg);
        }

        this.requestTimer = new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("BookieRequestTimer-%d").build(),
                this.serverCfg.getRequestTimerTickDurationMs(),
                TimeUnit.MILLISECONDS, this.serverCfg.getRequestTimerNumTicks());

        if (waitTimeoutOnBackpressureMillis > 0) {
            blacklistedChannels = Optional.of(CacheBuilder.newBuilder()
                    .expireAfterWrite(waitTimeoutOnBackpressureMillis, TimeUnit.MILLISECONDS)
                    .build());
        } else {
            blacklistedChannels = Optional.empty();
        }

        if (serverCfg.getCloseChannelOnResponseTimeout()) {
            onResponseTimeout = (ch) -> {
                LOG.warn("closing channel {} because it was non-writable for longer than {} ms",
                        ch, waitTimeoutOnBackpressureMillis);
                ch.close();
            };
        } else {
            // noop
            onResponseTimeout = (ch) -> {};
        }

        // Expose Stats
        this.statsEnabled = serverCfg.isStatisticsEnabled();
        this.addEntryStats = statsLogger.getOpStatsLogger(ADD_ENTRY);
        this.addRequestStats = statsLogger.getOpStatsLogger(ADD_ENTRY_REQUEST);
        this.readEntryStats = statsLogger.getOpStatsLogger(READ_ENTRY);
        this.forceLedgerStats = statsLogger.getOpStatsLogger(FORCE_LEDGER);
        this.forceLedgerRequestStats = statsLogger.getOpStatsLogger(FORCE_LEDGER_REQUEST);
        this.readRequestStats = statsLogger.getOpStatsLogger(READ_ENTRY_REQUEST);
        this.fenceReadEntryStats = statsLogger.getOpStatsLogger(READ_ENTRY_FENCE_READ);
        this.fenceReadRequestStats = statsLogger.getOpStatsLogger(READ_ENTRY_FENCE_REQUEST);
        this.fenceReadWaitStats = statsLogger.getOpStatsLogger(READ_ENTRY_FENCE_WAIT);
        this.readEntrySchedulingDelayStats = statsLogger.getOpStatsLogger(READ_ENTRY_SCHEDULING_DELAY);
        this.longPollPreWaitStats = statsLogger.getOpStatsLogger(READ_ENTRY_LONG_POLL_PRE_WAIT);
        this.longPollWaitStats = statsLogger.getOpStatsLogger(READ_ENTRY_LONG_POLL_WAIT);
        this.longPollReadStats = statsLogger.getOpStatsLogger(READ_ENTRY_LONG_POLL_READ);
        this.longPollReadRequestStats = statsLogger.getOpStatsLogger(READ_ENTRY_LONG_POLL_REQUEST);
        this.readLastEntryNoEntryErrorCounter = statsLogger.getCounter(READ_LAST_ENTRY_NOENTRY_ERROR);
        this.writeLacStats = statsLogger.getOpStatsLogger(WRITE_LAC);
        this.writeLacRequestStats = statsLogger.getOpStatsLogger(WRITE_LAC_REQUEST);
        this.readLacStats = statsLogger.getOpStatsLogger(READ_LAC);
        this.readLacRequestStats = statsLogger.getOpStatsLogger(READ_LAC_REQUEST);
        this.getBookieInfoStats = statsLogger.getOpStatsLogger(GET_BOOKIE_INFO);
        this.getBookieInfoRequestStats = statsLogger.getOpStatsLogger(GET_BOOKIE_INFO_REQUEST);
        this.channelWriteStats = statsLogger.getOpStatsLogger(CHANNEL_WRITE);

        this.addEntryBlockedStats = statsLogger.getOpStatsLogger(ADD_ENTRY_BLOCKED_WAIT);
        this.readEntryBlockedStats = statsLogger.getOpStatsLogger(READ_ENTRY_BLOCKED_WAIT);

        int maxAdds = serverCfg.getMaxAddsInProgressLimit();
        addsSemaphore = maxAdds > 0 ? new Semaphore(maxAdds, true) : null;

        int maxReads = serverCfg.getMaxReadsInProgressLimit();
        readsSemaphore = maxReads > 0 ? new Semaphore(maxReads, true) : null;

        statsLogger.registerGauge(ADD_ENTRY_IN_PROGRESS, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return addsInProgress;
            }
        });

        statsLogger.registerGauge(ADD_ENTRY_BLOCKED, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return addsBlocked;
            }
        });

        statsLogger.registerGauge(READ_ENTRY_IN_PROGRESS, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return readsInProgress;
            }
        });

        statsLogger.registerGauge(READ_ENTRY_BLOCKED, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return readsBlocked;
            }
        });

    }

    protected void onAddRequestStart(Channel channel) {
        if (addsSemaphore != null) {
            if (!addsSemaphore.tryAcquire()) {
                final long throttlingStartTimeNanos = MathUtils.nowInNano();
                channel.config().setAutoRead(false);
                LOG.info("Too many add requests in progress, disabling autoread on channel {}", channel);
                addsBlocked.incrementAndGet();
                addsSemaphore.acquireUninterruptibly();
                channel.config().setAutoRead(true);
                final long delayNanos = MathUtils.elapsedNanos(throttlingStartTimeNanos);
                LOG.info("Re-enabled autoread on channel {} after AddRequest delay of {} nanos", channel, delayNanos);
                addEntryBlockedStats.registerSuccessfulEvent(delayNanos, TimeUnit.NANOSECONDS);
                addsBlocked.decrementAndGet();
            }
        }
        final int curr = addsInProgress.incrementAndGet();
        maxAddsInProgress.accumulateAndGet(curr, Integer::max);
    }

    protected void onAddRequestFinish() {
        addsInProgress.decrementAndGet();
        if (addsSemaphore != null) {
            addsSemaphore.release();
        }
    }

    protected void onReadRequestStart(Channel channel) {
        if (readsSemaphore != null) {
            if (!readsSemaphore.tryAcquire()) {
                final long throttlingStartTimeNanos = MathUtils.nowInNano();
                channel.config().setAutoRead(false);
                LOG.info("Too many read requests in progress, disabling autoread on channel {}", channel);
                readsBlocked.incrementAndGet();
                readsSemaphore.acquireUninterruptibly();
                channel.config().setAutoRead(true);
                final long delayNanos = MathUtils.elapsedNanos(throttlingStartTimeNanos);
                LOG.info("Re-enabled autoread on channel {} after ReadRequest delay of {} nanos", channel, delayNanos);
                readEntryBlockedStats.registerSuccessfulEvent(delayNanos, TimeUnit.NANOSECONDS);
                readsBlocked.decrementAndGet();
            }
        }
        final int curr = readsInProgress.incrementAndGet();
        maxReadsInProgress.accumulateAndGet(curr, Integer::max);
    }

    protected void onReadRequestFinish() {
        readsInProgress.decrementAndGet();
        if (readsSemaphore != null) {
            readsSemaphore.release();
        }
    }

    @VisibleForTesting
    int maxAddsInProgressCount() {
        return maxAddsInProgress.get();
    }

    @VisibleForTesting
    int maxReadsInProgressCount() {
        return maxReadsInProgress.get();
    }

    @Override
    public void close() {
        shutdownExecutor(writeThreadPool);
        shutdownExecutor(readThreadPool);
        if (serverCfg.getNumLongPollWorkerThreads() > 0 || readThreadPool == null) {
            shutdownExecutor(longPollThreadPool);
        }
        shutdownExecutor(highPriorityThreadPool);
        requestTimer.stop();
    }

    private OrderedExecutor createExecutor(
            int numThreads,
            String nameFormat,
            int maxTasksInQueue,
            StatsLogger statsLogger) {
        if (numThreads <= 0) {
            return null;
        } else {
            return OrderedExecutor.newBuilder()
                    .numThreads(numThreads)
                    .name(nameFormat)
                    .traceTaskExecution(serverCfg.getEnableTaskExecutionStats())
                    .statsLogger(statsLogger)
                    .maxTasksInQueue(maxTasksInQueue)
                    .build();
        }
    }

    private void shutdownExecutor(OrderedExecutor service) {
        if (null != service) {
            service.shutdown();
        }
    }

    @Override
    public void processRequest(Object msg, Channel c) {
        // If we can decode this packet as a Request protobuf packet, process
        // it as a version 3 packet. Else, just use the old protocol.
        if (msg instanceof BookkeeperProtocol.Request) {
            BookkeeperProtocol.Request r = (BookkeeperProtocol.Request) msg;
            BookkeeperProtocol.BKPacketHeader header = r.getHeader();
            switch (header.getOperation()) {
                case ADD_ENTRY:
                    processAddRequestV3(r, c);
                    break;
                case READ_ENTRY:
                    processReadRequestV3(r, c);
                    break;
                case FORCE_LEDGER:
                    processForceLedgerRequestV3(r, c);
                    break;
                case AUTH:
                    LOG.info("Ignoring auth operation from client {}", c.remoteAddress());
                    BookkeeperProtocol.AuthMessage message = BookkeeperProtocol.AuthMessage
                        .newBuilder()
                        .setAuthPluginName(AuthProviderFactoryFactory.AUTHENTICATION_DISABLED_PLUGIN_NAME)
                        .setPayload(ByteString.copyFrom(AuthToken.NULL.getData()))
                        .build();
                    BookkeeperProtocol.Response.Builder authResponse = BookkeeperProtocol.Response
                        .newBuilder().setHeader(r.getHeader())
                        .setStatus(BookkeeperProtocol.StatusCode.EOK)
                        .setAuthResponse(message);
                    c.writeAndFlush(authResponse.build());
                    break;
                case WRITE_LAC:
                    processWriteLacRequestV3(r, c);
                    break;
                case READ_LAC:
                    processReadLacRequestV3(r, c);
                    break;
                case GET_BOOKIE_INFO:
                    processGetBookieInfoRequestV3(r, c);
                    break;
                case START_TLS:
                    processStartTLSRequestV3(r, c);
                    break;
                default:
                    LOG.info("Unknown operation type {}", header.getOperation());
                    BookkeeperProtocol.Response.Builder response =
                            BookkeeperProtocol.Response.newBuilder().setHeader(r.getHeader())
                            .setStatus(BookkeeperProtocol.StatusCode.EBADREQ);
                    c.writeAndFlush(response.build());
                    if (statsEnabled) {
                        bkStats.getOpStats(BKStats.STATS_UNKNOWN).incrementFailedOps();
                    }
                    break;
            }
        } else {
            BookieProtocol.Request r = (BookieProtocol.Request) msg;
            // process packet
            switch (r.getOpCode()) {
                case BookieProtocol.ADDENTRY:
                    checkArgument(r instanceof BookieProtocol.ParsedAddRequest);
                    processAddRequest((BookieProtocol.ParsedAddRequest) r, c);
                    break;
                case BookieProtocol.READENTRY:
                    checkArgument(r instanceof BookieProtocol.ReadRequest);
                    processReadRequest((BookieProtocol.ReadRequest) r, c);
                    break;
                default:
                    LOG.error("Unknown op type {}, sending error", r.getOpCode());
                    c.writeAndFlush(ResponseBuilder.buildErrorResponse(BookieProtocol.EBADREQ, r));
                    if (statsEnabled) {
                        bkStats.getOpStats(BKStats.STATS_UNKNOWN).incrementFailedOps();
                    }
                    break;
            }
        }
    }

     private void processWriteLacRequestV3(final BookkeeperProtocol.Request r, final Channel c) {
        WriteLacProcessorV3 writeLac = new WriteLacProcessorV3(r, c, this);
        if (null == writeThreadPool) {
            writeLac.run();
        } else {
            writeThreadPool.executeOrdered(r.getAddRequest().getLedgerId(), writeLac);
        }
    }

    private void processReadLacRequestV3(final BookkeeperProtocol.Request r, final Channel c) {
        ReadLacProcessorV3 readLac = new ReadLacProcessorV3(r, c, this);
        if (null == readThreadPool) {
            readLac.run();
        } else {
            readThreadPool.executeOrdered(r.getAddRequest().getLedgerId(), readLac);
        }
    }

    private void processAddRequestV3(final BookkeeperProtocol.Request r, final Channel c) {
        WriteEntryProcessorV3 write = new WriteEntryProcessorV3(r, c, this);

        final OrderedExecutor threadPool;
        if (RequestUtils.isHighPriority(r)) {
            threadPool = highPriorityThreadPool;
        } else {
            threadPool = writeThreadPool;
        }

        if (null == threadPool) {
            write.run();
        } else {
            try {
                threadPool.executeOrdered(r.getAddRequest().getLedgerId(), write);
            } catch (RejectedExecutionException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Failed to process request to add entry at {}:{}. Too many pending requests",
                              r.getAddRequest().getLedgerId(), r.getAddRequest().getEntryId());
                }
                BookkeeperProtocol.AddResponse.Builder addResponse = BookkeeperProtocol.AddResponse.newBuilder()
                        .setLedgerId(r.getAddRequest().getLedgerId())
                        .setEntryId(r.getAddRequest().getEntryId())
                        .setStatus(BookkeeperProtocol.StatusCode.ETOOMANYREQUESTS);
                BookkeeperProtocol.Response.Builder response = BookkeeperProtocol.Response.newBuilder()
                        .setHeader(write.getHeader())
                        .setStatus(addResponse.getStatus())
                        .setAddResponse(addResponse);
                BookkeeperProtocol.Response resp = response.build();
                write.sendResponse(addResponse.getStatus(), resp, addRequestStats);
            }
        }
    }

    private void processForceLedgerRequestV3(final BookkeeperProtocol.Request r, final Channel c) {
        ForceLedgerProcessorV3 forceLedger = new ForceLedgerProcessorV3(r, c, this);

        final OrderedExecutor threadPool;
        if (RequestUtils.isHighPriority(r)) {
            threadPool = highPriorityThreadPool;
        } else {
            threadPool = writeThreadPool;
        }

        if (null == threadPool) {
            forceLedger.run();
        } else {
            try {
                threadPool.executeOrdered(r.getForceLedgerRequest().getLedgerId(), forceLedger);
            } catch (RejectedExecutionException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Failed to process request to force ledger {}. Too many pending requests",
                              r.getForceLedgerRequest().getLedgerId());
                }
                BookkeeperProtocol.ForceLedgerResponse.Builder forceLedgerResponse =
                        BookkeeperProtocol.ForceLedgerResponse.newBuilder()
                        .setLedgerId(r.getForceLedgerRequest().getLedgerId())
                        .setStatus(BookkeeperProtocol.StatusCode.ETOOMANYREQUESTS);
                BookkeeperProtocol.Response.Builder response = BookkeeperProtocol.Response.newBuilder()
                        .setHeader(forceLedger.getHeader())
                        .setStatus(forceLedgerResponse.getStatus())
                        .setForceLedgerResponse(forceLedgerResponse);
                BookkeeperProtocol.Response resp = response.build();
                forceLedger.sendResponse(forceLedgerResponse.getStatus(), resp, forceLedgerRequestStats);
            }
        }
    }

    private void processReadRequestV3(final BookkeeperProtocol.Request r, final Channel c) {
        ExecutorService fenceThread = null == highPriorityThreadPool ? null : highPriorityThreadPool.chooseThread(c);

        final ReadEntryProcessorV3 read;
        final OrderedExecutor threadPool;
        if (RequestUtils.isLongPollReadRequest(r.getReadRequest())) {
            ExecutorService lpThread = longPollThreadPool.chooseThread(c);

            read = new LongPollReadEntryProcessorV3(r, c, this, fenceThread,
                                                    lpThread, requestTimer);
            threadPool = longPollThreadPool;
        } else {
            read = new ReadEntryProcessorV3(r, c, this, fenceThread);

            // If it's a high priority read (fencing or as part of recovery process), we want to make sure it
            // gets executed as fast as possible, so bypass the normal readThreadPool
            // and execute in highPriorityThreadPool
            boolean isHighPriority = RequestUtils.isHighPriority(r)
                || hasFlag(r.getReadRequest(), BookkeeperProtocol.ReadRequest.Flag.FENCE_LEDGER);
            if (isHighPriority) {
                threadPool = highPriorityThreadPool;
            } else {
                threadPool = readThreadPool;
            }
        }

        if (null == threadPool) {
            read.run();
        } else {
            try {
                threadPool.executeOrdered(r.getReadRequest().getLedgerId(), read);
            } catch (RejectedExecutionException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Failed to process request to read entry at {}:{}. Too many pending requests",
                              r.getReadRequest().getLedgerId(), r.getReadRequest().getEntryId());
                }
                BookkeeperProtocol.ReadResponse.Builder readResponse = BookkeeperProtocol.ReadResponse.newBuilder()
                    .setLedgerId(r.getReadRequest().getLedgerId())
                    .setEntryId(r.getReadRequest().getEntryId())
                    .setStatus(BookkeeperProtocol.StatusCode.ETOOMANYREQUESTS);
                BookkeeperProtocol.Response.Builder response = BookkeeperProtocol.Response.newBuilder()
                    .setHeader(read.getHeader())
                    .setStatus(readResponse.getStatus())
                    .setReadResponse(readResponse);
                BookkeeperProtocol.Response resp = response.build();
                read.sendResponse(readResponse.getStatus(), resp, readRequestStats);
            }
        }
    }

    private void processStartTLSRequestV3(final BookkeeperProtocol.Request r, final Channel c) {
        BookkeeperProtocol.Response.Builder response = BookkeeperProtocol.Response.newBuilder();
        BookkeeperProtocol.BKPacketHeader.Builder header = BookkeeperProtocol.BKPacketHeader.newBuilder();
        header.setVersion(BookkeeperProtocol.ProtocolVersion.VERSION_THREE);
        header.setOperation(r.getHeader().getOperation());
        header.setTxnId(r.getHeader().getTxnId());
        response.setHeader(header.build());
        if (shFactory == null) {
            LOG.error("Got StartTLS request but TLS not configured");
            response.setStatus(BookkeeperProtocol.StatusCode.EBADREQ);
            c.writeAndFlush(response.build());
        } else {
            // there is no need to execute in a different thread as this operation is light
            SslHandler sslHandler = shFactory.newTLSHandler();
            c.pipeline().addFirst("tls", sslHandler);

            response.setStatus(BookkeeperProtocol.StatusCode.EOK);
            BookkeeperProtocol.StartTLSResponse.Builder builder = BookkeeperProtocol.StartTLSResponse.newBuilder();
            response.setStartTLSResponse(builder.build());
            sslHandler.handshakeFuture().addListener(new GenericFutureListener<Future<Channel>>() {
                @Override
                public void operationComplete(Future<Channel> future) throws Exception {
                    // notify the AuthPlugin the completion of the handshake, even in case of failure
                    AuthHandler.ServerSideHandler authHandler = c.pipeline()
                            .get(AuthHandler.ServerSideHandler.class);
                    authHandler.authProvider.onProtocolUpgrade();
                    if (future.isSuccess()) {
                        LOG.info("Session is protected by: {}", sslHandler.engine().getSession().getCipherSuite());
                    } else {
                        LOG.error("TLS Handshake failure: {}", future.cause());
                        BookkeeperProtocol.Response.Builder errResponse = BookkeeperProtocol.Response.newBuilder()
                                .setHeader(r.getHeader()).setStatus(BookkeeperProtocol.StatusCode.EIO);
                        c.writeAndFlush(errResponse.build());
                        if (statsEnabled) {
                            bkStats.getOpStats(BKStats.STATS_UNKNOWN).incrementFailedOps();
                        }
                    }
                }
            });
            c.writeAndFlush(response.build());
        }
    }

    private void processGetBookieInfoRequestV3(final BookkeeperProtocol.Request r, final Channel c) {
        GetBookieInfoProcessorV3 getBookieInfo = new GetBookieInfoProcessorV3(r, c, this);
        if (null == readThreadPool) {
            getBookieInfo.run();
        } else {
            readThreadPool.submit(getBookieInfo);
        }
    }

    private void processAddRequest(final BookieProtocol.ParsedAddRequest r, final Channel c) {
        WriteEntryProcessor write = WriteEntryProcessor.create(r, c, this);

        // If it's a high priority add (usually as part of recovery process), we want to make sure it gets
        // executed as fast as possible, so bypass the normal writeThreadPool and execute in highPriorityThreadPool
        final OrderedExecutor threadPool;
        if (r.isHighPriority()) {
            threadPool = highPriorityThreadPool;
        } else {
            threadPool = writeThreadPool;
        }

        if (null == threadPool) {
            write.run();
        } else {
            try {
                threadPool.executeOrdered(r.getLedgerId(), write);
            } catch (RejectedExecutionException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Failed to process request to add entry at {}:{}. Too many pending requests", r.ledgerId,
                            r.entryId);
                }

                write.sendResponse(BookieProtocol.ETOOMANYREQUESTS,
                        ResponseBuilder.buildErrorResponse(BookieProtocol.ETOOMANYREQUESTS, r), addRequestStats);
            }
        }
    }

    private void processReadRequest(final BookieProtocol.ReadRequest r, final Channel c) {
        ReadEntryProcessor read = ReadEntryProcessor.create(r, c, this);

        // If it's a high priority read (fencing or as part of recovery process), we want to make sure it
        // gets executed as fast as possible, so bypass the normal readThreadPool
        // and execute in highPriorityThreadPool
        final OrderedExecutor threadPool;
        if (r.isHighPriority() || r.isFencing()) {
            threadPool = highPriorityThreadPool;
        } else {
            threadPool = readThreadPool;
        }

        if (null == threadPool) {
            read.run();
        } else {
            try {
                threadPool.executeOrdered(r.getLedgerId(), read);
            } catch (RejectedExecutionException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Failed to process request to read entry at {}:{}. Too many pending requests", r.ledgerId,
                            r.entryId);
                }

                read.sendResponse(BookieProtocol.ETOOMANYREQUESTS,
                        ResponseBuilder.buildErrorResponse(BookieProtocol.ETOOMANYREQUESTS, r), readRequestStats);
            }
        }
    }

    public long getWaitTimeoutOnBackpressureMillis() {
        return waitTimeoutOnBackpressureMillis;
    }

    public void blacklistChannel(Channel channel) {
        blacklistedChannels
                .ifPresent(x -> x.put(channel, true));
    }

    public void invalidateBlacklist(Channel channel) {
        blacklistedChannels
                .ifPresent(x -> x.invalidate(channel));
    }

    public boolean isBlacklisted(Channel channel) {
        return blacklistedChannels
                .map(x -> x.getIfPresent(channel))
                .orElse(false);
    }

    public void handleNonWritableChannel(Channel channel) {
        onResponseTimeout.accept(channel);
    }
}
