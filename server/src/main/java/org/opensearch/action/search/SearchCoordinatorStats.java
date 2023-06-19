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

import java.util.function.Consumer;

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

    private void computeStats(SearchPhaseContext searchPhaseContext, Consumer<StatsHolder> consumer) {
        consumer.accept(totalStats);
    }

    @Override
    public void onQueryPhaseStart(SearchPhaseContext context) {
        computeStats(context, statsHolder -> {
            statsHolder.queryCurrent.inc();
        });
    }
    @Override
    public void onQueryPhaseEnd(SearchPhaseContext context, long tookTime) {
        computeStats(context, statsHolder -> {
            totalStats.queryCurrent.dec();
            totalStats.queryTotal.inc();
            totalStats.queryMetric.inc(tookTime);
        });
    }
    @Override
    public void onQueryPhaseFailure(SearchPhaseContext context) {
        return;
    }
    @Override
    public void onFetchPhaseStart(SearchPhaseContext context) {
        computeStats(context, statsHolder -> {
            totalStats.fetchCurrent.inc();
        });
    }
    @Override
    public void onFetchPhaseEnd(SearchPhaseContext context, long tookTime) {
        computeStats(context, statsHolder -> {
            totalStats.fetchCurrent.dec();
            totalStats.fetchTotal.inc();
            totalStats.fetchMetric.inc(tookTime);
        });
    }
    @Override
    public void onFetchPhaseFailure(SearchPhaseContext context) {
        return;
    }
    @Override
    public void onExpandSearchPhaseStart(SearchPhaseContext context) {
        computeStats(context, statsHolder -> {
            totalStats.expandSearchCurrent.inc();
        });
    }
    @Override
    public void onExpandSearchPhaseEnd(SearchPhaseContext context, long tookTime) {
        computeStats(context, statsHolder -> {
            totalStats.expandSearchCurrent.dec();
            totalStats.expandSearchTotal.inc();
            totalStats.expandSearchMetric.inc(tookTime);
        });
    }
    @Override
    public void onExpandSearchPhaseFailure(SearchPhaseContext context) {
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
