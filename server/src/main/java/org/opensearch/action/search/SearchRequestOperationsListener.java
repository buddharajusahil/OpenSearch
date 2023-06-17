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
     */
    void onQueryPhaseStart();
    void onQueryPhaseFailure();
    void onQueryPhaseEnd(long tookTime);
    void onFetchPhaseStart();
    void onFetchPhaseFailure();
    void onFetchPhaseEnd(long tookTime);
    void onExpandSearchPhaseStart();
    void onExpandSearchPhaseFailure();
    void onExpandSearchPhaseEnd(long tookTime);
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
        public void onQueryPhaseStart() {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    queryPhaseStart = System.nanoTime();
                    listener.onQueryPhaseStart();
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseStart listener [{}] failed", listener), e);
                }
            }
        }
        @Override
        public void onQueryPhaseEnd(long tookTime) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    queryPhaseEnd = System.nanoTime();
                    queryTotal = TimeUnit.NANOSECONDS.toMillis(queryPhaseEnd - queryPhaseStart);
                    listener.onQueryPhaseEnd(tookTime);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseEnd listener [{}] failed", listener), e);
                }
            }
        }
        @Override
        public void onQueryPhaseFailure() {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onQueryPhaseFailure();
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onPhaseFailure listener [{}] failed", listener), e);
                }
            }
        }
        @Override
        public void onFetchPhaseStart() {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    fetchPhaseStart = System.nanoTime();
                    listener.onFetchPhaseStart();
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onFetchStart listener [{}] failed", listener), e);
                }
            }
        }
        @Override
        public void onFetchPhaseEnd(long tookTime) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    fetchPhaseEnd = System.nanoTime();
                    fetchTotal = TimeUnit.NANOSECONDS.toMillis(fetchPhaseEnd - fetchPhaseStart);
                    listener.onFetchPhaseEnd(tookTime);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onFetchEnd listener [{}] failed", listener), e);
                }
            }
        }
        @Override
        public void onFetchPhaseFailure() {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onFetchPhaseFailure();
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onFetchFailure listener [{}] failed", listener), e);
                }
            }
        }
        @Override
        public void onExpandSearchPhaseStart() {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    expandSearchPhaseStart = System.nanoTime();
                    listener.onExpandSearchPhaseStart();
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onExpandSearchStart listener [{}] failed", listener), e);
                }
            }
        }
        @Override
        public void onExpandSearchPhaseEnd(long tookTime) {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    expandSearchPhaseEnd = System.nanoTime();
                    expandSearchTotal = TimeUnit.NANOSECONDS.toMillis(expandSearchPhaseEnd - expandSearchPhaseStart);
                    listener.onExpandSearchPhaseEnd(tookTime);
                } catch (Exception e) {
                    logger.warn(() -> new ParameterizedMessage("onExpandSearchEnd listener [{}] failed", listener), e);
                }
            }
        }
        @Override
        public void onExpandSearchPhaseFailure() {
            for (SearchRequestOperationsListener listener : listeners) {
                try {
                    listener.onExpandSearchPhaseFailure();
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
