/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.metrics.MeanMetric;

import java.util.function.Consumer;
public class SearchCoordinatorStatsTests extends OpenSearchTestCase{
    public void testSearchCoordinatorStats() {
        SearchCoordinatorStats testCoordinatorStats = new SearchCoordinatorStats();

        SearchPhaseContext ctx = new MockSearchPhaseContext(1);
        long tookTime = 10;

        testCoordinatorStats.onDFSPreQueryPhaseStart(ctx);
        assertEquals(1, testCoordinatorStats.getDFSPreQueryCurrent());

        testCoordinatorStats.onDFSPreQueryPhaseEnd(ctx, 10);
        assertEquals(0, testCoordinatorStats.getDFSPreQueryCurrent());
        assertEquals(1, testCoordinatorStats.getDFSPreQueryTotal());
        assertEquals(tookTime, testCoordinatorStats.getDFSPreQueryMetric());

        testCoordinatorStats.onCanMatchPhaseStart(ctx);
        assertEquals(1, testCoordinatorStats.getCanMatchCurrent());

        testCoordinatorStats.onCanMatchPhaseEnd(ctx, 10);
        assertEquals(0, testCoordinatorStats.getCanMatchCurrent());
        assertEquals(1, testCoordinatorStats.getCanMatchTotal());
        assertEquals(tookTime, testCoordinatorStats.getCanMatchMetric());

        testCoordinatorStats.onQueryPhaseStart(ctx);
        assertEquals(1, testCoordinatorStats.getQueryCurrent());

        testCoordinatorStats.onQueryPhaseEnd(ctx, 10);
        assertEquals(0, testCoordinatorStats.getQueryCurrent());
        assertEquals(1, testCoordinatorStats.getQueryTotal());
        assertEquals(tookTime, testCoordinatorStats.getQueryMetric());

        testCoordinatorStats.onFetchPhaseStart(ctx);
        assertEquals(1, testCoordinatorStats.getFetchCurrent());

        testCoordinatorStats.onFetchPhaseEnd(ctx, 10);
        assertEquals(0, testCoordinatorStats.getFetchCurrent());
        assertEquals(1, testCoordinatorStats.getFetchTotal());
        assertEquals(tookTime, testCoordinatorStats.getFetchMetric());

        testCoordinatorStats.onExpandSearchPhaseStart(ctx);
        assertEquals(1, testCoordinatorStats.getExpandSearchCurrent());

        testCoordinatorStats.onExpandSearchPhaseEnd(ctx, 10);
        assertEquals(0, testCoordinatorStats.getExpandSearchCurrent());
        assertEquals(1, testCoordinatorStats.getExpandSearchTotal());
        assertEquals(tookTime, testCoordinatorStats.getExpandSearchMetric());

    }
}
