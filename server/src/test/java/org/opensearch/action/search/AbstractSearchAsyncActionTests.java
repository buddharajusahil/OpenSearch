/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.search;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.junit.After;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.common.UUIDs;
import org.opensearch.common.breaker.CircuitBreaker;
import org.opensearch.common.breaker.NoopCircuitBreaker;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.common.Strings;
import org.opensearch.index.Index;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.search.*;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.InternalAggregationTestCase;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

public class AbstractSearchAsyncActionTests extends OpenSearchTestCase {

    private final List<Tuple<String, String>> resolvedNodes = new ArrayList<>();
    private final Set<ShardSearchContextId> releasedContexts = new CopyOnWriteArraySet<>();
    private ExecutorService executor;

    ThreadPool threadPool;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        executor = Executors.newFixedThreadPool(1);
        threadPool = new TestThreadPool(getClass().getName());
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
        ThreadPool.terminate(threadPool, 5, TimeUnit.SECONDS);
    }

    private AbstractSearchAsyncAction<SearchPhaseResult> createAction(
        SearchRequest request,
        ArraySearchPhaseResults<SearchPhaseResult> results,
        ActionListener<SearchResponse> listener,
        final boolean controlled,
        final AtomicLong expected
    ) {
        return createAction(
            request,
            results,
            listener,
            controlled,
            false,
            expected,
            new SearchShardIterator(null, null, Collections.emptyList(), null)
        );
    }

    private AbstractSearchAsyncAction<SearchPhaseResult> createAction(
        SearchRequest request,
        ArraySearchPhaseResults<SearchPhaseResult> results,
        ActionListener<SearchResponse> listener,
        final boolean controlled,
        final boolean failExecutePhaseOnShard,
        final AtomicLong expected,
        final SearchShardIterator... shards
    ) {

        final Runnable runnable;
        final TransportSearchAction.SearchTimeProvider timeProvider;
        if (controlled) {
            runnable = () -> expected.set(randomNonNegativeLong());
            timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, expected::get);
        } else {
            runnable = () -> {
                long elapsed = spinForAtLeastNMilliseconds(randomIntBetween(1, 10));
                expected.set(elapsed);
            };
            timeProvider = new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(), System::nanoTime);
        }

        BiFunction<String, String, Transport.Connection> nodeIdToConnection = (cluster, node) -> {
            resolvedNodes.add(Tuple.tuple(cluster, node));
            return null;
        };

        return new AbstractSearchAsyncAction<SearchPhaseResult>(
            "test",
            logger,
            null,
            nodeIdToConnection,
            Collections.singletonMap("foo", new AliasFilter(new MatchAllQueryBuilder())),
            Collections.singletonMap("foo", 2.0f),
            Collections.singletonMap("name", Sets.newHashSet("bar", "baz")),
            executor,
            request,
            listener,
            new GroupShardsIterator<>(Arrays.asList(shards)),
            timeProvider,
            ClusterState.EMPTY_STATE,
            null,
            results,
            request.getMaxConcurrentShardRequests(),
            SearchResponse.Clusters.EMPTY
        ) {
            @Override
            protected SearchPhase getNextPhase(final SearchPhaseResults<SearchPhaseResult> results, SearchPhaseContext context) {
                return null;
            }

            @Override
            protected void executePhaseOnShard(
                final SearchShardIterator shardIt,
                final SearchShardTarget shard,
                final SearchActionListener<SearchPhaseResult> listener
            ) {
                if (failExecutePhaseOnShard) {
                    listener.onFailure(new ShardNotFoundException(shardIt.shardId()));
                } else {
                    listener.onResponse(new QuerySearchResult());
                }
            }

            @Override
            long buildTookInMillis() {
                runnable.run();
                return super.buildTookInMillis();
            }

            @Override
            public void sendReleaseSearchContext(
                ShardSearchContextId contextId,
                Transport.Connection connection,
                OriginalIndices originalIndices
            ) {
                releasedContexts.add(contextId);
            }
        };
    }

    public void testTookWithControlledClock() {
        runTestTook(true);
    }

    public void testTookWithRealClock() {
        runTestTook(false);
    }

    private void runTestTook(final boolean controlled) {
        final AtomicLong expected = new AtomicLong();
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(
            new SearchRequest(),
            new ArraySearchPhaseResults<>(10),
            null,
            controlled,
            expected
        );
        final long actual = action.buildTookInMillis();
        if (controlled) {
            // with a controlled clock, we can assert the exact took time
            assertThat(actual, equalTo(TimeUnit.NANOSECONDS.toMillis(expected.get())));
        } else {
            // with a real clock, the best we can say is that it took as long as we spun for
            assertThat(actual, greaterThanOrEqualTo(TimeUnit.NANOSECONDS.toMillis(expected.get())));
        }
    }

    public void testBuildShardSearchTransportRequest() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(randomBoolean()).preference("_shards:1,3");
        final AtomicLong expected = new AtomicLong();
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(
            searchRequest,
            new ArraySearchPhaseResults<>(10),
            null,
            false,
            expected
        );
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
        SearchShardIterator iterator = new SearchShardIterator(
            clusterAlias,
            new ShardId(new Index("name", "foo"), 1),
            Collections.emptyList(),
            new OriginalIndices(new String[] { "name", "name1" }, IndicesOptions.strictExpand())
        );
        ShardSearchRequest shardSearchTransportRequest = action.buildShardSearchRequest(iterator);
        assertEquals(IndicesOptions.strictExpand(), shardSearchTransportRequest.indicesOptions());
        assertArrayEquals(new String[] { "name", "name1" }, shardSearchTransportRequest.indices());
        assertEquals(new MatchAllQueryBuilder(), shardSearchTransportRequest.getAliasFilter().getQueryBuilder());
        assertEquals(2.0f, shardSearchTransportRequest.indexBoost(), 0.0f);
        assertArrayEquals(new String[] { "name", "name1" }, shardSearchTransportRequest.indices());
        assertArrayEquals(new String[] { "bar", "baz" }, shardSearchTransportRequest.indexRoutings());
        assertEquals("_shards:1,3", shardSearchTransportRequest.preference());
        assertEquals(clusterAlias, shardSearchTransportRequest.getClusterAlias());
    }

    public void testBuildSearchResponse() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(randomBoolean());
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults = new ArraySearchPhaseResults<>(10);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest, phaseResults, null, false, new AtomicLong());
        InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
        SearchResponse searchResponse = action.buildSearchResponse(internalSearchResponse, action.buildShardFailures(), null, null);
        assertSame(searchResponse.getAggregations(), internalSearchResponse.aggregations());
        assertSame(searchResponse.getSuggest(), internalSearchResponse.suggest());
        assertSame(searchResponse.getProfileResults(), internalSearchResponse.profile());
        assertSame(searchResponse.getHits(), internalSearchResponse.hits());
    }

    public void testBuildSearchResponseAllowPartialFailures() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        final ArraySearchPhaseResults<SearchPhaseResult> queryResult = new ArraySearchPhaseResults<>(10);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest, queryResult, null, false, new AtomicLong());
        action.onShardFailure(
            0,
            new SearchShardTarget("node", new ShardId("index", "index-uuid", 0), null, OriginalIndices.NONE),
            new IllegalArgumentException()
        );
        InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
        SearchResponse searchResponse = action.buildSearchResponse(internalSearchResponse, action.buildShardFailures(), null, null);
        assertSame(searchResponse.getAggregations(), internalSearchResponse.aggregations());
        assertSame(searchResponse.getSuggest(), internalSearchResponse.suggest());
        assertSame(searchResponse.getProfileResults(), internalSearchResponse.profile());
        assertSame(searchResponse.getHits(), internalSearchResponse.hits());
    }

    public void testSendSearchResponseDisallowPartialFailures() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<SearchResponse> listener = ActionListener.wrap(response -> fail("onResponse should not be called"), exception::set);
        Set<ShardSearchContextId> requestIds = new HashSet<>();
        List<Tuple<String, String>> nodeLookups = new ArrayList<>();
        int numFailures = randomIntBetween(1, 5);
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults = phaseResults(requestIds, nodeLookups, numFailures);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest, phaseResults, listener, false, new AtomicLong());
        for (int i = 0; i < numFailures; i++) {
            ShardId failureShardId = new ShardId("index", "index-uuid", i);
            String failureClusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
            String failureNodeId = randomAlphaOfLengthBetween(5, 10);
            action.onShardFailure(
                i,
                new SearchShardTarget(failureNodeId, failureShardId, failureClusterAlias, OriginalIndices.NONE),
                new IllegalArgumentException()
            );
        }
        action.sendSearchResponse(InternalSearchResponse.empty(), phaseResults.results);
        assertThat(exception.get(), instanceOf(SearchPhaseExecutionException.class));
        SearchPhaseExecutionException searchPhaseExecutionException = (SearchPhaseExecutionException) exception.get();
        assertEquals(0, searchPhaseExecutionException.getSuppressed().length);
        assertEquals(numFailures, searchPhaseExecutionException.shardFailures().length);
        for (ShardSearchFailure shardSearchFailure : searchPhaseExecutionException.shardFailures()) {
            assertThat(shardSearchFailure.getCause(), instanceOf(IllegalArgumentException.class));
        }
        assertEquals(nodeLookups, resolvedNodes);
        assertEquals(requestIds, releasedContexts);
    }

    public void testOnPhaseFailure() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<SearchResponse> listener = ActionListener.wrap(response -> fail("onResponse should not be called"), exception::set);
        Set<ShardSearchContextId> requestIds = new HashSet<>();
        List<Tuple<String, String>> nodeLookups = new ArrayList<>();
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults = phaseResults(requestIds, nodeLookups, 0);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest, phaseResults, listener, false, new AtomicLong());
        action.onPhaseFailure(new SearchPhase("test") {
            @Override
            public void run() {

            }
        }, "message", null);
        assertThat(exception.get(), instanceOf(SearchPhaseExecutionException.class));
        SearchPhaseExecutionException searchPhaseExecutionException = (SearchPhaseExecutionException) exception.get();
        assertEquals("message", searchPhaseExecutionException.getMessage());
        assertEquals("test", searchPhaseExecutionException.getPhaseName());
        assertEquals(0, searchPhaseExecutionException.shardFailures().length);
        assertEquals(0, searchPhaseExecutionException.getSuppressed().length);
        assertEquals(nodeLookups, resolvedNodes);
        assertEquals(requestIds, releasedContexts);
    }

    public void testShardNotAvailableWithDisallowPartialFailures() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<SearchResponse> listener = ActionListener.wrap(response -> fail("onResponse should not be called"), exception::set);
        int numShards = randomIntBetween(2, 10);
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults = new ArraySearchPhaseResults<>(numShards);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest, phaseResults, listener, false, new AtomicLong());
        // skip one to avoid the "all shards failed" failure.
        SearchShardIterator skipIterator = new SearchShardIterator(null, null, Collections.emptyList(), null);
        skipIterator.resetAndSkip();
        action.skipShard(skipIterator);
        // expect at least 2 shards, so onPhaseDone should report failure.
        action.onPhaseDone();
        assertThat(exception.get(), instanceOf(SearchPhaseExecutionException.class));
        SearchPhaseExecutionException searchPhaseExecutionException = (SearchPhaseExecutionException) exception.get();
        assertEquals("Partial shards failure (" + (numShards - 1) + " shards unavailable)", searchPhaseExecutionException.getMessage());
        assertEquals("test", searchPhaseExecutionException.getPhaseName());
        assertEquals(0, searchPhaseExecutionException.shardFailures().length);
        assertEquals(0, searchPhaseExecutionException.getSuppressed().length);
    }

    private static ArraySearchPhaseResults<SearchPhaseResult> phaseResults(
        Set<ShardSearchContextId> contextIds,
        List<Tuple<String, String>> nodeLookups,
        int numFailures
    ) {
        int numResults = randomIntBetween(1, 10);
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults = new ArraySearchPhaseResults<>(numResults + numFailures);

        for (int i = 0; i < numResults; i++) {
            ShardSearchContextId contextId = new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong());
            contextIds.add(contextId);
            SearchPhaseResult phaseResult = new PhaseResult(contextId);
            String resultClusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
            String resultNodeId = randomAlphaOfLengthBetween(5, 10);
            ShardId resultShardId = new ShardId("index", "index-uuid", i);
            nodeLookups.add(Tuple.tuple(resultClusterAlias, resultNodeId));
            phaseResult.setSearchShardTarget(new SearchShardTarget(resultNodeId, resultShardId, resultClusterAlias, OriginalIndices.NONE));
            phaseResult.setShardIndex(i);
            phaseResults.consumeResult(phaseResult, () -> {});
        }
        return phaseResults;
    }

    public void testOnShardFailurePhaseDoneFailure() throws InterruptedException {
        final Index index = new Index("test", UUID.randomUUID().toString());
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean fail = new AtomicBoolean(true);

        final SearchShardIterator[] shards = IntStream.range(0, 5 + randomInt(10))
            .mapToObj(i -> new SearchShardIterator(null, new ShardId(index, i), List.of("n1", "n2", "n3"), null, null, null))
            .toArray(SearchShardIterator[]::new);

        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        searchRequest.setMaxConcurrentShardRequests(1);

        final ArraySearchPhaseResults<SearchPhaseResult> queryResult = new ArraySearchPhaseResults<>(shards.length);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(
            searchRequest,
            queryResult,
            new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {

                }

                @Override
                public void onFailure(Exception e) {
                    if (fail.compareAndExchange(true, false)) {
                        try {
                            throw new RuntimeException("Simulated exception");
                        } finally {
                            executor.submit(() -> latch.countDown());
                        }
                    }
                }
            },
            false,
            true,
            new AtomicLong(),
            shards
        );
        action.run();
        assertTrue(latch.await(1, TimeUnit.SECONDS));

        InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
        SearchResponse searchResponse = action.buildSearchResponse(internalSearchResponse, action.buildShardFailures(), null, null);
        assertSame(searchResponse.getAggregations(), internalSearchResponse.aggregations());
        assertSame(searchResponse.getSuggest(), internalSearchResponse.suggest());
        assertSame(searchResponse.getProfileResults(), internalSearchResponse.profile());
        assertSame(searchResponse.getHits(), internalSearchResponse.hits());
        assertThat(searchResponse.getSuccessfulShards(), equalTo(0));
    }

    public void testOnShardSuccessPhaseDoneFailure() throws InterruptedException {
        final Index index = new Index("test", UUID.randomUUID().toString());
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean fail = new AtomicBoolean(true);

        final SearchShardIterator[] shards = IntStream.range(0, 5 + randomInt(10))
            .mapToObj(i -> new SearchShardIterator(null, new ShardId(index, i), List.of("n1", "n2", "n3"), null, null, null))
            .toArray(SearchShardIterator[]::new);

        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        searchRequest.setMaxConcurrentShardRequests(1);

        final ArraySearchPhaseResults<SearchPhaseResult> queryResult = new ArraySearchPhaseResults<>(shards.length);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(
            searchRequest,
            queryResult,
            new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {
                    if (fail.compareAndExchange(true, false)) {
                        throw new RuntimeException("Simulated exception");
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    executor.submit(() -> latch.countDown());
                }
            },
            false,
            false,
            new AtomicLong(),
            shards
        );
        action.run();
        assertTrue(latch.await(1, TimeUnit.SECONDS));

        InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
        SearchResponse searchResponse = action.buildSearchResponse(internalSearchResponse, action.buildShardFailures(), null, null);
        assertSame(searchResponse.getAggregations(), internalSearchResponse.aggregations());
        assertSame(searchResponse.getSuggest(), internalSearchResponse.suggest());
        assertSame(searchResponse.getProfileResults(), internalSearchResponse.profile());
        assertSame(searchResponse.getHits(), internalSearchResponse.hits());
        assertThat(searchResponse.getSuccessfulShards(), equalTo(shards.length));
    }

    public void testExecutePhaseOnShardFailure() throws InterruptedException {
        final Index index = new Index("test", UUID.randomUUID().toString());

        final SearchShardIterator[] shards = IntStream.range(0, 2 + randomInt(3))
            .mapToObj(i -> new SearchShardIterator(null, new ShardId(index, i), List.of("n1", "n2", "n3"), null, null, null))
            .toArray(SearchShardIterator[]::new);

        final AtomicBoolean fail = new AtomicBoolean(true);
        final CountDownLatch latch = new CountDownLatch(1);
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        searchRequest.setMaxConcurrentShardRequests(5);

        final ArraySearchPhaseResults<SearchPhaseResult> queryResult = new ArraySearchPhaseResults<>(shards.length);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(
            searchRequest,
            queryResult,
            new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {}

                @Override
                public void onFailure(Exception e) {
                    try {
                        // We end up here only when onPhaseDone() is called (causing NPE) and
                        // ending up in the onPhaseFailure() callback
                        if (fail.compareAndExchange(true, false)) {
                            assertThat(e, instanceOf(SearchPhaseExecutionException.class));
                            throw new RuntimeException("Simulated exception");
                        }
                    } finally {
                        executor.submit(() -> latch.countDown());
                    }
                }
            },
            false,
            false,
            new AtomicLong(),
            shards
        );
        action.run();
        assertTrue(latch.await(1, TimeUnit.SECONDS));

        InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
        SearchResponse searchResponse = action.buildSearchResponse(internalSearchResponse, action.buildShardFailures(), null, null);
        assertSame(searchResponse.getAggregations(), internalSearchResponse.aggregations());
        assertSame(searchResponse.getSuggest(), internalSearchResponse.suggest());
        assertSame(searchResponse.getProfileResults(), internalSearchResponse.profile());
        assertSame(searchResponse.getHits(), internalSearchResponse.hits());
        assertThat(searchResponse.getSuccessfulShards(), equalTo(shards.length));
    }



    public void testSearchRequestListeners() throws InterruptedException{
        AtomicInteger dfsPreQueryPhaseStart = new AtomicInteger();
        AtomicInteger dfsPreQueryPhaseFailure = new AtomicInteger();
        AtomicInteger dfsPreQueryPhaseEnd = new AtomicInteger();
        AtomicInteger canMatchPhaseStart = new AtomicInteger();
        AtomicInteger canMatchPhaseFailure = new AtomicInteger();
        AtomicInteger canMatchPhaseEnd = new AtomicInteger();
        AtomicInteger queryPhaseStart = new AtomicInteger();
        AtomicInteger queryPhaseFailure = new AtomicInteger();
        AtomicInteger queryPhaseEnd = new AtomicInteger();
        AtomicInteger fetchPhaseStart = new AtomicInteger();
        AtomicInteger fetchPhaseFailure = new AtomicInteger();
        AtomicInteger fetchPhaseEnd = new AtomicInteger();
        AtomicInteger expandPhaseStart = new AtomicInteger();
        AtomicInteger expandPhaseFailure = new AtomicInteger();
        AtomicInteger expandPhaseEnd = new AtomicInteger();
        AtomicInteger timeInNanos = new AtomicInteger(randomIntBetween(0, 10));

        SearchRequestOperationsListener testListener = new SearchRequestOperationsListener() {
            @Override
            public void onDFSPreQueryPhaseStart(SearchPhaseContext context) {
                assertNotNull(context);
                dfsPreQueryPhaseStart.incrementAndGet();
            }

            @Override
            public void onDFSPreQueryPhaseFailure(SearchPhaseContext context) {
                assertNotNull(context);
                dfsPreQueryPhaseFailure.incrementAndGet();
            }

            @Override
            public void onDFSPreQueryPhaseEnd(SearchPhaseContext context, long tookTime) {
                assertNotNull(context);
                dfsPreQueryPhaseEnd.incrementAndGet();
            }

            @Override
            public void onCanMatchPhaseStart(SearchPhaseContext context) {
                assertNotNull(context);
                canMatchPhaseStart.incrementAndGet();
            }

            @Override
            public void onCanMatchPhaseFailure(SearchPhaseContext context) {
                assertNotNull(context);
                canMatchPhaseFailure.incrementAndGet();
            }

            @Override
            public void onCanMatchPhaseEnd(SearchPhaseContext context, long tookTime) {
                assertNotNull(context);
                canMatchPhaseEnd.incrementAndGet();
            }

            @Override
            public void onQueryPhaseStart(SearchPhaseContext context) {
                assertNotNull(context);
                queryPhaseStart.incrementAndGet();
            }

            @Override
            public void onQueryPhaseFailure(SearchPhaseContext context) {
                assertNotNull(context);
                queryPhaseFailure.incrementAndGet();
            }

            @Override
            public void onQueryPhaseEnd(SearchPhaseContext context, long tookTime) {
                assertNotNull(context);
                queryPhaseEnd.incrementAndGet();
            }

            @Override
            public void onFetchPhaseStart(SearchPhaseContext context) {
                assertNotNull(context);
                fetchPhaseStart.incrementAndGet();
            }

            @Override
            public void onFetchPhaseFailure(SearchPhaseContext context) {
                assertNotNull(context);
                fetchPhaseFailure.incrementAndGet();
            }

            @Override
            public void onFetchPhaseEnd(SearchPhaseContext context, long tookTime) {
                assertNotNull(context);
                fetchPhaseEnd.incrementAndGet();
            }

            @Override
            public void onExpandSearchPhaseStart(SearchPhaseContext context) {
                assertNotNull(context);
                expandPhaseStart.incrementAndGet();
            }

            @Override
            public void onExpandSearchPhaseFailure(SearchPhaseContext context) {
                assertNotNull(context);
                expandPhaseFailure.incrementAndGet();
            }

            @Override
            public void onExpandSearchPhaseEnd(SearchPhaseContext context, long tookTime) {
                assertNotNull(context);
                expandPhaseEnd.incrementAndGet();
            }
        };
        final List<SearchRequestOperationsListener> requestOperationListeners = new ArrayList<>(Arrays.asList(testListener, testListener));


        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
            0,
            System.nanoTime(),
            System::nanoTime
        );
        SearchTransportService searchTransportService = new SearchTransportService(null, null);
        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        SearchPhaseController controller = new SearchPhaseController(
            writableRegistry(),
            r -> InternalAggregationTestCase.emptyReduceContextBuilder()
        );
        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(false);

        int numConcurrent = randomIntBetween(1, 4);
        searchRequest.setMaxConcurrentShardRequests(numConcurrent);
        searchRequest.setBatchedReduceSize(2);
        searchRequest.source(new SearchSourceBuilder().size(1).sort(SortBuilders.fieldSort("timestamp")));
        SearchTask task = new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap());
        int numShards = 1;
        Executor executor = OpenSearchExecutors.newDirectExecutorService();
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        GroupShardsIterator<SearchShardIterator> shardsIter = SearchAsyncActionTests.getShardsIter(
            "idx",
            new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS),
            numShards,
            randomBoolean(),
            primaryNode,
            replicaNode
        );
        QueryPhaseResultConsumer resultConsumer = new QueryPhaseResultConsumer(
            searchRequest,
            executor,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST),
            controller,
            task.getProgressListener(),
            writableRegistry(),
            shardsIter.size(),
            exc -> {}
        );

        SearchQueryThenFetchAsyncAction action = new SearchQueryThenFetchAsyncAction(
            logger,
            searchTransportService,
            (clusterAlias, node) -> lookup.get(node),
            Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY)),
            Collections.emptyMap(),
            Collections.emptyMap(),
            controller,
            executor,
            resultConsumer,
            searchRequest,
            null,
            shardsIter,
            timeProvider,
            null,
            task,
            SearchResponse.Clusters.EMPTY
        );
        action.setSearchListenerList(requestOperationListeners);

        SearchDfsQueryThenFetchAsyncAction searchDfsQueryThenFetchAsyncAction = new SearchDfsQueryThenFetchAsyncAction(
            logger,
            searchTransportService,
            (clusterAlias, node) -> lookup.get(node),
            Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY)),
            Collections.emptyMap(),
            Collections.emptyMap(),
            controller,
            executor,
            resultConsumer,
            searchRequest,
            null,
            shardsIter,
            timeProvider,
            null,
            task,
            SearchResponse.Clusters.EMPTY
        );
        searchDfsQueryThenFetchAsyncAction.setSearchListenerList(requestOperationListeners);

        CanMatchPreFilterSearchPhase canMatchPreFilterSearchPhaseAction = new CanMatchPreFilterSearchPhase(
            logger,
            searchTransportService,
            (clusterAlias, node) -> lookup.get(node),
            Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY)),
            Collections.emptyMap(),
            Collections.emptyMap(),
            OpenSearchExecutors.newDirectExecutorService(),
            searchRequest,
            null,
            shardsIter,
            timeProvider,
            ClusterState.EMPTY_STATE,
            null,
            null,
            SearchResponse.Clusters.EMPTY
        );
        canMatchPreFilterSearchPhaseAction.setSearchListenerList(requestOperationListeners);

        action.start();
        assertEquals(2, queryPhaseStart.get());

        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        QueryPhaseResultConsumer results = controller.newSearchPhaseResults(
            OpenSearchExecutors.newDirectExecutorService(),
            new NoopCircuitBreaker(CircuitBreaker.REQUEST),
            SearchProgressListener.NOOP,
            mockSearchPhaseContext.getRequest(),
            1,
            exc -> {}
        );

        FetchSearchPhase fetchPhase = new FetchSearchPhase(
            results,
            controller,
            null,
            mockSearchPhaseContext,
            (searchResponse, scrollId) -> new SearchPhase("test") {
                @Override
                public void run() {
                    mockSearchPhaseContext.sendSearchResponse(searchResponse, null);
                }
            }
        );
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomInt());
        SearchShardIterator searchShardIterator = new SearchShardIterator(null, shardId, Collections.emptyList(), OriginalIndices.NONE);
        searchShardIterator.resetAndSkip();
        action.skipShard(searchShardIterator);

        action.executeNextPhase(action, fetchPhase);

        assertEquals(2, queryPhaseEnd.get());

        searchDfsQueryThenFetchAsyncAction.start();
        searchDfsQueryThenFetchAsyncAction.skipShard(searchShardIterator);
        searchDfsQueryThenFetchAsyncAction.executeNextPhase(searchDfsQueryThenFetchAsyncAction, fetchPhase);

        canMatchPreFilterSearchPhaseAction.start();

        assertEquals(2, dfsPreQueryPhaseStart.get());
        assertEquals(2, dfsPreQueryPhaseEnd.get());
        assertEquals(2, canMatchPhaseStart.get());
        assertEquals(4, fetchPhaseStart.get());

        String collapseValue = randomBoolean() ? null : "boom";
        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(null, null, null, null, false, null, 1);
        ExpandSearchPhase expandPhase = new ExpandSearchPhase(mockSearchPhaseContext, internalSearchResponse, null);
        action.executeNextPhase(fetchPhase, expandPhase);
        assertEquals(2, expandPhaseStart.get());
    }
    public void testMultiThreadCoordinateStats() throws InterruptedException {
        AtomicInteger dfsPreQueryPhaseStart = new AtomicInteger();
        AtomicInteger dfsPreQueryPhaseFailure = new AtomicInteger();
        AtomicInteger dfsPreQueryPhaseEnd = new AtomicInteger();
        AtomicInteger canMatchPhaseStart = new AtomicInteger();
        AtomicInteger canMatchPhaseFailure = new AtomicInteger();
        AtomicInteger canMatchPhaseEnd = new AtomicInteger();
        AtomicInteger queryPhaseStart = new AtomicInteger();
        AtomicInteger queryPhaseFailure = new AtomicInteger();
        AtomicInteger queryPhaseEnd = new AtomicInteger();
        AtomicInteger fetchPhaseStart = new AtomicInteger();
        AtomicInteger fetchPhaseFailure = new AtomicInteger();
        AtomicInteger fetchPhaseEnd = new AtomicInteger();
        AtomicInteger expandPhaseStart = new AtomicInteger();
        AtomicInteger expandPhaseFailure = new AtomicInteger();
        AtomicInteger expandPhaseEnd = new AtomicInteger();
        AtomicInteger timeInNanos = new AtomicInteger(randomIntBetween(0, 10));

        SearchRequestOperationsListener testListener = new SearchRequestOperationsListener() {
            @Override
            public void onDFSPreQueryPhaseStart(SearchPhaseContext context) {
                assertNotNull(context);
                dfsPreQueryPhaseStart.incrementAndGet();
            }

            @Override
            public void onDFSPreQueryPhaseFailure(SearchPhaseContext context) {
                assertNotNull(context);
                dfsPreQueryPhaseFailure.incrementAndGet();
            }

            @Override
            public void onDFSPreQueryPhaseEnd(SearchPhaseContext context, long tookTime) {
                assertEquals(timeInNanos.get(), tookTime);
                assertNotNull(context);
                dfsPreQueryPhaseEnd.incrementAndGet();
            }

            @Override
            public void onCanMatchPhaseStart(SearchPhaseContext context) {
                assertNotNull(context);
                canMatchPhaseStart.incrementAndGet();
            }

            @Override
            public void onCanMatchPhaseFailure(SearchPhaseContext context) {
                assertNotNull(context);
                canMatchPhaseFailure.incrementAndGet();
            }

            @Override
            public void onCanMatchPhaseEnd(SearchPhaseContext context, long tookTime) {
                assertNotNull(context);
                canMatchPhaseEnd.incrementAndGet();
            }

            @Override
            public void onQueryPhaseStart(SearchPhaseContext context) {
                assertNotNull(context);
                queryPhaseStart.incrementAndGet();
            }

            @Override
            public void onQueryPhaseFailure(SearchPhaseContext context) {
                assertNotNull(context);
                queryPhaseFailure.incrementAndGet();
            }

            @Override
            public void onQueryPhaseEnd(SearchPhaseContext context, long tookTime) {
                assertNotNull(context);
                queryPhaseEnd.incrementAndGet();
            }

            @Override
            public void onFetchPhaseStart(SearchPhaseContext context) {
                assertNotNull(context);
                fetchPhaseStart.incrementAndGet();
            }

            @Override
            public void onFetchPhaseFailure(SearchPhaseContext context) {
                assertNotNull(context);
                fetchPhaseFailure.incrementAndGet();
            }

            @Override
            public void onFetchPhaseEnd(SearchPhaseContext context, long tookTime) {
                assertNotNull(context);
                fetchPhaseEnd.incrementAndGet();
            }

            @Override
            public void onExpandSearchPhaseStart(SearchPhaseContext context) {
                assertNotNull(context);
                expandPhaseStart.incrementAndGet();
            }

            @Override
            public void onExpandSearchPhaseFailure(SearchPhaseContext context) {
                assertNotNull(context);
                expandPhaseFailure.incrementAndGet();
            }

            @Override
            public void onExpandSearchPhaseEnd(SearchPhaseContext context, long tookTime) {
                assertNotNull(context);
                expandPhaseEnd.incrementAndGet();
            }
        };
        final List<SearchRequestOperationsListener> requestOperationListeners = new ArrayList<>(Arrays.asList(testListener, testListener));

        int numTasks = randomIntBetween(5, 50);

        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
            0,
            System.nanoTime(),
            System::nanoTime
        );
        SearchTransportService searchTransportService = new SearchTransportService(null, null);
        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        SearchPhaseController controller = new SearchPhaseController(
            writableRegistry(),
            r -> InternalAggregationTestCase.emptyReduceContextBuilder()
        );
        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(false);

        int numConcurrent = randomIntBetween(1, 4);
        searchRequest.setMaxConcurrentShardRequests(numConcurrent);
        searchRequest.setBatchedReduceSize(2);
        searchRequest.source(new SearchSourceBuilder().size(1).sort(SortBuilders.fieldSort("timestamp")));
        SearchTask task = new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap());
        int numShards = 1;
        Executor executor = OpenSearchExecutors.newDirectExecutorService();
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        GroupShardsIterator<SearchShardIterator> shardsIter = SearchAsyncActionTests.getShardsIter(
            "idx",
            new OriginalIndices(new String[]{"idx"}, SearchRequest.DEFAULT_INDICES_OPTIONS),
            numShards,
            randomBoolean(),
            primaryNode,
            replicaNode
        );
        QueryPhaseResultConsumer resultConsumer = new QueryPhaseResultConsumer(
            searchRequest,
            executor,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST),
            controller,
            task.getProgressListener(),
            writableRegistry(),
            shardsIter.size(),
            exc -> {
            }
        );
        MockSearchPhaseContext mockSearchPhaseContext = new MockSearchPhaseContext(1);
        QueryPhaseResultConsumer results = controller.newSearchPhaseResults(
            OpenSearchExecutors.newDirectExecutorService(),
            new NoopCircuitBreaker(CircuitBreaker.REQUEST),
            SearchProgressListener.NOOP,
            mockSearchPhaseContext.getRequest(),
            1,
            exc -> {}
        );


        Thread[] threads = new Thread[numTasks];
        Phaser phaser = new Phaser(numTasks + 1);
        CountDownLatch countDownLatch = new CountDownLatch(numTasks);

        Thread[] threadsFetch = new Thread[numTasks];
        Phaser phaserFetch = new Phaser(numTasks + 1);
        CountDownLatch countDownLatchFetch = new CountDownLatch(numTasks);

        for (int i = 0; i < numTasks; i++) {
            SearchQueryThenFetchAsyncAction newAction = new SearchQueryThenFetchAsyncAction(
                logger,
                searchTransportService,
                (clusterAlias, node) -> lookup.get(node),
                Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY)),
                Collections.emptyMap(),
                Collections.emptyMap(),
                controller,
                executor,
                resultConsumer,
                searchRequest,
                null,
                shardsIter,
                timeProvider,
                null,
                task,
                SearchResponse.Clusters.EMPTY
            );
            newAction.setSearchListenerList(requestOperationListeners);
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                newAction.start();
                countDownLatch.countDown();
            });
            threads[i].start();

            FetchSearchPhase fetchPhase = new FetchSearchPhase(
                results,
                controller,
                null,
                mockSearchPhaseContext,
                (searchResponse, scrollId) -> new SearchPhase("test") {
                    @Override
                    public void run() {
                        mockSearchPhaseContext.sendSearchResponse(searchResponse, null);
                    }
                }
            );
            ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomInt());
            SearchShardIterator searchShardIterator = new SearchShardIterator(null, shardId, Collections.emptyList(), OriginalIndices.NONE);
            searchShardIterator.resetAndSkip();
            newAction.skipShard(searchShardIterator);

            threadsFetch[i] = new Thread(() -> {
                phaserFetch.arriveAndAwaitAdvance();
                newAction.executeNextPhase(newAction, fetchPhase);
                countDownLatchFetch.countDown();
            });
            threadsFetch[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        countDownLatch.await();
        assertEquals(numTasks * 2, queryPhaseStart.get());

        phaserFetch.arriveAndAwaitAdvance();
        countDownLatchFetch.await();

        assertEquals(numTasks * 2, queryPhaseEnd.get());
        assertEquals(numTasks * 2, fetchPhaseStart.get());
        Thread[] threadsDFS = new Thread[numTasks];
        Phaser phaserDFS = new Phaser(numTasks + 1);
        CountDownLatch countDownLatchDFS = new CountDownLatch(numTasks);

        for (int i = 0; i < numTasks; i++) {
            SearchDfsQueryThenFetchAsyncAction newAction = new SearchDfsQueryThenFetchAsyncAction(
                logger,
                searchTransportService,
                (clusterAlias, node) -> lookup.get(node),
                Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY)),
                Collections.emptyMap(),
                Collections.emptyMap(),
                controller,
                executor,
                resultConsumer,
                searchRequest,
                null,
                shardsIter,
                timeProvider,
                null,
                task,
                SearchResponse.Clusters.EMPTY
            );
            newAction.setSearchListenerList(requestOperationListeners);
            threadsDFS[i] = new Thread(() -> {
                phaserDFS.arriveAndAwaitAdvance();
                newAction.start();
                countDownLatchDFS.countDown();
            });
            threadsDFS[i].start();


        }
        phaserDFS.arriveAndAwaitAdvance();
        countDownLatchDFS.await();
        assertEquals(numTasks * 2, dfsPreQueryPhaseStart.get());

    }
    private CountDownLatch runActionsConcurrently(List<? extends AbstractSearchAsyncAction<SearchPhaseResult>> listOfActions) {
        Thread[] threads = new Thread[5];
        Phaser phaser = new Phaser(5 + 1);
        CountDownLatch countDownLatch = new CountDownLatch(5);
        for (int i = 0; i < 5; i++) {
            int index = i;
            threads[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                listOfActions.get(index).start();
                countDownLatch.countDown();
            });
            threads[i].start();
        }
        phaser.arriveAndAwaitAdvance();
        return countDownLatch;
    }

    private static final class PhaseResult extends SearchPhaseResult {
        PhaseResult(ShardSearchContextId contextId) {
            this.contextId = contextId;
        }
    }
}
