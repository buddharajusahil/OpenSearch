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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.search.stats;

import org.opensearch.action.search.SearchCoordinatorStats;
import org.opensearch.index.search.stats.SearchStats.Stats;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class SearchStatsTests extends OpenSearchTestCase {

    // https://github.com/elastic/elasticsearch/issues/7644
    public void testShardLevelSearchGroupStats() throws Exception {
        // let's create two dummy search stats with groups
        Map<String, Stats> groupStats1 = new HashMap<>();
        Map<String, Stats> groupStats2 = new HashMap<>();
        groupStats2.put("group1", new Stats(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1));
        SearchStats searchStats1 = new SearchStats(new Stats(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), 0, groupStats1);
        SearchStats searchStats2 = new SearchStats(new Stats(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), 0, groupStats2);

        // adding these two search stats and checking group stats are correct
        searchStats1.add(searchStats2);
        assertStats(groupStats1.get("group1"), 1);

        // another call, adding again ...
        searchStats1.add(searchStats2);
        assertStats(groupStats1.get("group1"), 2);

        // making sure stats2 was not affected (this would previously return 2!)
        assertStats(groupStats2.get("group1"), 1);

        // adding again would then return wrong search stats (would return 4! instead of 3)
        searchStats1.add(searchStats2);
        assertStats(groupStats1.get("group1"), 3);

        long paramValue = 1;

        // Testing for coordinator stats
        SearchCoordinatorStats testCoordinatorStats = new SearchCoordinatorStats();
        testCoordinatorStats.totalStats.dfsPreQueryMetric.inc(paramValue);
        testCoordinatorStats.totalStats.dfsPreQueryCurrent.inc(paramValue);
        testCoordinatorStats.totalStats.dfsPreQueryTotal.inc(paramValue);
        testCoordinatorStats.totalStats.canMatchMetric.inc(paramValue);
        testCoordinatorStats.totalStats.canMatchCurrent.inc(paramValue);
        testCoordinatorStats.totalStats.canMatchTotal.inc(paramValue);
        testCoordinatorStats.totalStats.queryMetric.inc(paramValue);
        testCoordinatorStats.totalStats.queryCurrent.inc(paramValue);
        testCoordinatorStats.totalStats.queryTotal.inc(paramValue);
        testCoordinatorStats.totalStats.fetchMetric.inc(paramValue);
        testCoordinatorStats.totalStats.fetchCurrent.inc(paramValue);
        testCoordinatorStats.totalStats.fetchTotal.inc(paramValue);
        testCoordinatorStats.totalStats.expandSearchMetric.inc(paramValue);
        testCoordinatorStats.totalStats.expandSearchCurrent.inc(paramValue);
        testCoordinatorStats.totalStats.expandSearchTotal.inc(paramValue);

        searchStats1.setSearchCoordinatorStats(testCoordinatorStats);
        assertCoordinatorStats(searchStats1.getTotal(), paramValue);

    }

    private static void assertStats(Stats stats, long equalTo) {
        assertEquals(equalTo, stats.getQueryCount());
        assertEquals(equalTo, stats.getQueryTimeInMillis());
        assertEquals(equalTo, stats.getQueryCurrent());
        assertEquals(equalTo, stats.getFetchCount());
        assertEquals(equalTo, stats.getFetchTimeInMillis());
        assertEquals(equalTo, stats.getFetchCurrent());
        assertEquals(equalTo, stats.getScrollCount());
        assertEquals(equalTo, stats.getScrollTimeInMillis());
        assertEquals(equalTo, stats.getScrollCurrent());
        assertEquals(equalTo, stats.getPitCount());
        assertEquals(equalTo, stats.getPitTimeInMillis());
        assertEquals(equalTo, stats.getPitCurrent());
        assertEquals(equalTo, stats.getSuggestCount());
        assertEquals(equalTo, stats.getSuggestTimeInMillis());
        assertEquals(equalTo, stats.getSuggestCurrent());
    }

    private static void assertCoordinatorStats(Stats stats, long equalTo) {
        assertEquals(equalTo, stats.getCoordinatorStatsLongHolder().dfsPreQueryMetric);
        assertEquals(equalTo, stats.getCoordinatorStatsLongHolder().dfsPreQueryCurrent);
        assertEquals(equalTo, stats.getCoordinatorStatsLongHolder().dfsPreQueryTotal);
        assertEquals(equalTo, stats.getCoordinatorStatsLongHolder().canMatchMetric);
        assertEquals(equalTo, stats.getCoordinatorStatsLongHolder().canMatchCurrent);
        assertEquals(equalTo, stats.getCoordinatorStatsLongHolder().canMatchTotal);
        assertEquals(equalTo, stats.getCoordinatorStatsLongHolder().queryMetric);
        assertEquals(equalTo, stats.getCoordinatorStatsLongHolder().queryCurrent);
        assertEquals(equalTo, stats.getCoordinatorStatsLongHolder().queryTotal);
        assertEquals(equalTo, stats.getCoordinatorStatsLongHolder().fetchMetric);
        assertEquals(equalTo, stats.getCoordinatorStatsLongHolder().fetchCurrent);
        assertEquals(equalTo, stats.getCoordinatorStatsLongHolder().fetchTotal);
        assertEquals(equalTo, stats.getCoordinatorStatsLongHolder().expandSearchMetric);
        assertEquals(equalTo, stats.getCoordinatorStatsLongHolder().expandSearchCurrent);
        assertEquals(equalTo, stats.getCoordinatorStatsLongHolder().expandSearchTotal);
    }

}
