/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.inject.Inject;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.metrics.MeanMetric;

/**
 * Coordinator level search stats
 *
 * @opensearch.internal
 */
public final class SearchCoordinatorStats implements SearchRequestOperationsListener {
    public StatsHolder totalStats = new StatsHolder();

    // private final CounterMetric openContexts = new CounterMetric();

    // private volatile Map<String, StatsHolder> groupStats = emptyMap();
    private long queryPhaseStart;
    private long queryPhaseEnd;
    private long fetchPhaseStart;
    private long fetchPhaseEnd;
    private long expandSearchPhaseStart;
    private long expandSearchPhaseEnd;

    public long getQueryPhaseStart() {
        return queryPhaseStart;
    }
    public long getQueryPhaseEnd() {
        return queryPhaseEnd;
    }
    public long getFetchPhaseStart() {
        return fetchPhaseStart;
    }
    public long getFetchPhaseEnd() {
        return fetchPhaseEnd;
    }
    public long getExpandSearchPhaseStart() {
        return expandSearchPhaseStart;
    }
    public long getExpandSearchPhaseEnd() {
        return expandSearchPhaseEnd;
    }

    @Inject
    public SearchCoordinatorStats () { }
    public long getQueryMetric() {
        return totalStats.queryMetric.sum();
    }
    public long getQueryCurrent() {
        return totalStats.queryCurrent.count();
    }
    public long getQueryTotal() {
        return totalStats.queryTotal.count();
    }
    public long getFetchMetric() {
        return totalStats.fetchMetric.sum();
    }
    public long getFetchCurrent() {
        return totalStats.fetchCurrent.count();
    }
    public long getFetchTotal() {
        return totalStats.fetchTotal.count();
    }
    public long getExpandSearchMetric() {
        return totalStats.expandSearchMetric.sum();
    }
    public long getExpandSearchCurrent() {
        return totalStats.expandSearchCurrent.count();
    }
    public long getExpandSearchTotal() {
        return totalStats.expandSearchTotal.count();
    }
    public void incQueryCurrent() { totalStats.queryCurrent.inc(); }
    public void decQueryCurrent() { totalStats.queryCurrent.dec(); }
    public void incQueryTotal() { totalStats.queryTotal.inc(); }
    public void incFetchCurrent() { totalStats.fetchCurrent.inc(); }
    public void decFetchCurrent() { totalStats.fetchCurrent.dec(); }
    public void incFetchTotal() { totalStats.fetchTotal.inc(); }
    public void incExpandSearchCurrent() { totalStats.expandSearchCurrent.inc(); }
    public void decExpandSearch() { totalStats.expandSearchCurrent.dec(); }
    public void incExpandSearch() { totalStats.expandSearchTotal.inc(); }

    public void setStats(SearchCoordinatorStats searchCoordinatorStats) {
        totalStats.queryMetric.inc(searchCoordinatorStats.totalStats.queryMetric.sum());
        totalStats.queryCurrent.inc(searchCoordinatorStats.totalStats.queryCurrent.count());
        totalStats.queryTotal.inc(searchCoordinatorStats.totalStats.queryTotal.count());
        totalStats.fetchMetric.inc(searchCoordinatorStats.totalStats.fetchMetric.sum());
        totalStats.fetchCurrent.inc(searchCoordinatorStats.totalStats.fetchCurrent.count());
        totalStats.fetchTotal.inc(searchCoordinatorStats.totalStats.fetchTotal.count());
        totalStats.expandSearchMetric.inc(searchCoordinatorStats.totalStats.expandSearchMetric.sum());
        totalStats.expandSearchCurrent.inc(searchCoordinatorStats.totalStats.expandSearchCurrent.count());
        totalStats.expandSearchTotal.inc(searchCoordinatorStats.totalStats.expandSearchTotal.count());
    }
    @Override
    public void onQueryPhaseStart() {
        totalStats.queryCurrent.inc();
    }
    @Override
    public void onQueryPhaseEnd() {
        totalStats.queryCurrent.dec();
        totalStats.queryTotal.inc();
    }
    @Override
    public void onQueryPhaseFailure() {
        return;
    }
    @Override
    public void onFetchPhaseStart() {
        totalStats.fetchCurrent.inc();
    }
    @Override
    public void onFetchPhaseEnd() {
        totalStats.fetchCurrent.dec();
    }
    @Override
    public void onFetchPhaseFailure() {
        return;
    }
    @Override
    public void onExpandSearchPhaseStart() {
        totalStats.expandSearchCurrent.inc();
    }
    @Override
    public void onExpandSearchPhaseEnd() {
        totalStats.expandSearchCurrent.dec();
        totalStats.expandSearchTotal.inc();
    }
    @Override
    public void onExpandSearchPhaseFailure() {
        return;
    }
    @Override
    public void addQueryTotal(long queryTotal) {
        totalStats.queryMetric.inc(queryTotal);
    }
    @Override
    public void addFetchTotal(long fetchTotal) {
        totalStats.fetchMetric.inc(fetchTotal);
    }
    @Override
    public void addExpandSearchTotal(long expandSearchTotal) {
       totalStats.expandSearchMetric.inc(expandSearchTotal);
    }

    public static final class StatsHolder {
        public MeanMetric queryMetric = new MeanMetric();
        public CounterMetric queryCurrent = new CounterMetric();
        public CounterMetric queryTotal = new CounterMetric();
        public MeanMetric fetchMetric = new MeanMetric();
        public CounterMetric fetchCurrent = new CounterMetric();
        public CounterMetric fetchTotal = new CounterMetric();
        public MeanMetric expandSearchMetric = new MeanMetric();
        public CounterMetric expandSearchCurrent = new CounterMetric();
        public CounterMetric expandSearchTotal = new CounterMetric();
    }

}
