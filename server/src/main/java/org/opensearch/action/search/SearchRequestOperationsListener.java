/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import org.opensearch.ExceptionsHelper;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.transport.TransportRequest;
import org.opensearch.action.search.SearchPhaseContext;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A listener for search, fetch and context events at the coordinator node level
 *
 * @opensearch.internal
 */
public interface SearchRequestOperationsListener {

    /**
     * Executed when the request is started
     * @param context the current searchPhase context
     */
    // void onRequestStart(SearchPhaseContext context);

    /**
     * Executed when the request is ended
     * @param context the current searchPhase context
     */
    // void onRequestEnd(SearchPhaseContext context);

    /**
     * Executed when the query phase is started
     * @param context the current searchPhase context
     */
    void onQueryPhaseStart(SearchPhaseContext context);
    void onQueryPhaseFailure(SearchPhaseContext context);
    void onQueryPhaseEnd(SearchPhaseContext context);
    void onFetchPhaseStart(SearchPhaseContext context);
    void onFetchPhaseFailure(SearchPhaseContext context);
    void onFetchPhaseEnd(SearchPhaseContext context);
    void onExpandSearchPhaseStart(SearchPhaseContext context);
    void onExpandSearchPhaseFailure(SearchPhaseContext context);
    void onExpandSearchPhaseEnd(SearchPhaseContext context);
    void addQueryTotal(long queryTotal);
    void addFetchTotal(long fetchTotal);
    void addExpandSearchTotal(long expandSearchTotal);



    final class CompositeListener implements SearchRequestOperationsListener {
        private final List<SearchRequestOperationsListener> listeners;
        private final Logger logger;
        private long queryPhaseStart;
        private long queryPhaseEnd;
        private long fetchPhaseStart;
        private long fetchPhaseEnd;
        private long expandSearchPhaseStart;
        private long expandSearchPhaseEnd;
        private long queryTotal;
        private long fetchTotal;
        private long expandSearchTotal;


        public CompositeListener(List<SearchRequestOperationsListener> listeners, Logger logger) {
            this.listeners = listeners;
            this.logger = logger;
        }
        @Override
        public void onQueryPhaseStart(SearchPhaseContext context) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    queryPhaseStart = System.nanoTime();
                    listener.onQueryPhaseStart(context);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseStart listener [{}] failed", listener), e);
                }
            }
        }
        @Override
        public void onQueryPhaseEnd(SearchPhaseContext context) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    queryPhaseEnd = System.nanoTime();
                    queryTotal = TimeUnit.NANOSECONDS.toMillis(queryPhaseEnd - queryPhaseStart);
                    listener.onQueryPhaseEnd(context);
                    listener.addQueryTotal(queryTotal);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseEnd listener [{}] failed", listener), e);
                }
            }
        }
        @Override
        public void onQueryPhaseFailure(SearchPhaseContext searchPhaseContext) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onQueryPhaseFailure(searchPhaseContext);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseFailure listener [{}] failed", listener), e);
                }
            }
        }
        @Override
        public void onFetchPhaseStart(SearchPhaseContext context) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    fetchPhaseStart = System.nanoTime();
                    listener.onFetchPhaseStart(context);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onFetchStart listener [{}] failed", listener), e);
                }
            }
        }
        @Override
        public void onFetchPhaseEnd(SearchPhaseContext context) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    fetchPhaseEnd = System.nanoTime();
                    fetchTotal = TimeUnit.NANOSECONDS.toMillis(fetchPhaseEnd - fetchPhaseStart);
                    listener.onFetchPhaseEnd(context);
                    listener.addFetchTotal(fetchTotal);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onFetchEnd listener [{}] failed", listener), e);
                }
            }
        }
        @Override
        public void onFetchPhaseFailure(SearchPhaseContext searchPhaseContext) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onFetchPhaseFailure(searchPhaseContext);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onFetchFailure listener [{}] failed", listener), e);
                }
            }
        }
        @Override
        public void onExpandSearchPhaseStart(SearchPhaseContext context) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    expandSearchPhaseStart = System.nanoTime();
                    listener.onExpandSearchPhaseStart(context);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onExpandSearchStart listener [{}] failed", listener), e);
                }
            }
        }
        @Override
        public void onExpandSearchPhaseEnd(SearchPhaseContext context) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    expandSearchPhaseEnd = System.nanoTime();
                    expandSearchTotal = TimeUnit.NANOSECONDS.toMillis(expandSearchPhaseEnd - expandSearchPhaseStart);
                    listener.onExpandSearchPhaseEnd(context);
                    listener.addExpandSearchTotal(expandSearchTotal);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onExpandSearchEnd listener [{}] failed", listener), e);
                }
            }
        }
        @Override
        public void onExpandSearchPhaseFailure(SearchPhaseContext searchPhaseContext) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onExpandSearchPhaseFailure(searchPhaseContext);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onExpandSearchFailure listener [{}] failed", listener), e);
                }
            }
        }
        @Override
        public void addQueryTotal(long queryTotal) {
            this.queryTotal = queryTotal;
        }
        @Override
        public void addFetchTotal(long fetchTotal) {
            this.fetchTotal = fetchTotal;
        }
        @Override
        public void addExpandSearchTotal(long expandSearchTotal) {
            this.expandSearchTotal = expandSearchTotal;
        }
    }



}
