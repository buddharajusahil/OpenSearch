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
        testCoordinatorStats.totalStats.dfsPreQueryMetric.setSum(paramValue);
        testCoordinatorStats.totalStats.dfsPreQueryCurrent.setCount(paramValue);
        testCoordinatorStats.totalStats.dfsPreQueryTotal.setCount(paramValue);
        testCoordinatorStats.totalStats.canMatchMetric.setSum(paramValue);
        testCoordinatorStats.totalStats.canMatchCurrent.setCount(paramValue);
        testCoordinatorStats.totalStats.canMatchTotal.setCount(paramValue);
        testCoordinatorStats.totalStats.queryMetric.setSum(paramValue);
        testCoordinatorStats.totalStats.queryCurrent.setCount(paramValue);
        testCoordinatorStats.totalStats.queryTotal.setCount(paramValue);
        testCoordinatorStats.totalStats.fetchMetric.setSum(paramValue);
        testCoordinatorStats.totalStats.fetchCurrent.setCount(paramValue);
        testCoordinatorStats.totalStats.fetchTotal.setCount(paramValue);
        testCoordinatorStats.totalStats.expandSearchMetric.setSum(paramValue);
        testCoordinatorStats.totalStats.expandSearchCurrent.setCount(paramValue);
        testCoordinatorStats.totalStats.expandSearchTotal.setCount(paramValue);

        searchStats1.setSearchCoordinatorStats(testCoordinatorStats);
        assertCoordinatorStats(searchStats1.getTotal(), 1);

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
        assertEquals(equalTo, stats.getSearchCoordinatorStats().getDFSPreQueryMetric());
        assertEquals(equalTo, stats.getSearchCoordinatorStats().getDFSPreQueryCurrent());
        assertEquals(equalTo, stats.getSearchCoordinatorStats().getDFSPreQueryTotal());
        assertEquals(equalTo, stats.getSearchCoordinatorStats().getCanMatchMetric());
        assertEquals(equalTo, stats.getSearchCoordinatorStats().getCanMatchCurrent());
        assertEquals(equalTo, stats.getSearchCoordinatorStats().getCanMatchTotal());
        assertEquals(equalTo, stats.getSearchCoordinatorStats().getQueryMetric());
        assertEquals(equalTo, stats.getSearchCoordinatorStats().getQueryCurrent());
        assertEquals(equalTo, stats.getSearchCoordinatorStats().getQueryTotal());
        assertEquals(equalTo, stats.getSearchCoordinatorStats().getFetchMetric());
        assertEquals(equalTo, stats.getSearchCoordinatorStats().getFetchCurrent());
        assertEquals(equalTo, stats.getSearchCoordinatorStats().getFetchTotal());
        assertEquals(equalTo, stats.getSearchCoordinatorStats().getExpandSearchMetric());
        assertEquals(equalTo, stats.getSearchCoordinatorStats().getExpandSearchCurrent());
        assertEquals(equalTo, stats.getSearchCoordinatorStats().getExpandSearchTotal());
    }

}
