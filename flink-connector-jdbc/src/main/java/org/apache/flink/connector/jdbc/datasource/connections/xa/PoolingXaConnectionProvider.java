/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.datasource.connections.xa;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.datasource.transactions.xa.exceptions.TransientXaException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.sql.XADataSource;
import javax.transaction.xa.Xid;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.flink.util.ExceptionUtils.rethrow;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A "pooling" implementation of {@link XaConnectionProvider}. Some database implement XA such that
 * one connection is limited to a single transaction. As a workaround, this implementation creates a
 * new XA resource after each xa_start call is made (and associates it with the xid to commit
 * later).
 */
@Internal
public class PoolingXaConnectionProvider implements XaConnectionProvider {
    private static final long serialVersionUID = 1L;

    /** A supplier of connection provider. */
    public interface ConnectionProviderSupplier
            extends Serializable, Supplier<XaConnectionProvider> {}

    private static final Logger LOG = LoggerFactory.getLogger(PoolingXaConnectionProvider.class);
    private final ConnectionProviderSupplier providerSupplier;
    private transient XaConnectionProvider active;
    private transient Map<Xid, XaConnectionProvider> mappedToXids;
    private transient Deque<XaConnectionProvider> pooled;

    public static PoolingXaConnectionProvider from(
            Supplier<XADataSource> dataSourceSupplier, Integer timeoutSec) {
        return from(() -> SimpleXaConnectionProvider.from(dataSourceSupplier, timeoutSec));
    }

    public static PoolingXaConnectionProvider from(ConnectionProviderSupplier facadeSupplier) {
        return new PoolingXaConnectionProvider(facadeSupplier);
    }

    private PoolingXaConnectionProvider(ConnectionProviderSupplier facadeSupplier) {
        this.providerSupplier = facadeSupplier;
    }

    @Override
    public void open() throws Exception {
        checkState(active == null);
        pooled = new LinkedList<>();
        mappedToXids = new HashMap<>();
    }

    @Override
    public boolean isOpen() {
        return active != null && active.isOpen();
    }

    @Override
    public void start(Xid xid) throws Exception {
        checkState(active == null);
        if (pooled.isEmpty()) {
            active = providerSupplier.get();
            active.open();
        } else {
            active = pooled.poll();
        }
        active.start(xid);
        mappedToXids.put(xid, active);
    }

    /**
     * Must be called after {@link #start(Xid)} with the same {@link Xid}.
     *
     * @see XaConnectionProvider#endAndPrepare(Xid)
     */
    @Override
    public void endAndPrepare(Xid xid) throws Exception {
        checkState(active == mappedToXids.get(xid));
        try {
            active.endAndPrepare(xid);
        } finally {
            active = null;
        }
    }

    @Override
    public void commit(Xid xid, boolean ignoreUnknown) throws TransientXaException {
        runForXid(xid, facade -> facade.commit(xid, ignoreUnknown));
    }

    @Override
    public void rollback(Xid xid) throws TransientXaException {
        runForXid(xid, facade -> facade.rollback(xid));
    }

    @Override
    public void failAndRollback(Xid xid) throws TransientXaException {
        runForXid(xid, facade -> facade.failAndRollback(xid));
    }

    @Override
    public Collection<Xid> recover() throws TransientXaException {
        return peekPooled().recover();
    }

    @Override
    public void close() throws Exception {
        for (XaConnectionProvider facade : mappedToXids.values()) {
            facade.close();
        }
        for (XaConnectionProvider facade : pooled) {
            facade.close();
        }
        if (active != null && active.isOpen()) {
            active.close();
        }
    }

    @Nullable
    @Override
    public Connection getConnection() {
        return active.getConnection();
    }

    @Override
    public boolean isConnectionValid() throws SQLException {
        return active.isConnectionValid();
    }

    @Override
    public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {
        return active.getOrEstablishConnection();
    }

    @Override
    public void closeConnection() {
        active.closeConnection();
    }

    @Override
    public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
        return active.reestablishConnection();
    }

    // WARN: action MUST leave the facade in IDLE state (i.e. not start/end/prepare any tx)
    private void runForXid(
            Xid xid, ThrowingConsumer<XaConnectionProvider, TransientXaException> action) {
        XaConnectionProvider mapped = mappedToXids.remove(xid);
        if (mapped == null) {
            // a transaction can be not known during recovery
            LOG.debug("No XA resource found associated with XID: {}", xid);
            action.accept(peekPooled());
        } else {
            LOG.debug("Found mapped XA resource for XID: {} {}", xid, mapped);
            try {
                action.accept(mapped);
            } finally {
                pooled.offer(mapped);
            }
        }
    }

    // WARN: the returned facade MUST be left in IDLE state (i.e. not start/end/prepare any tx)
    private XaConnectionProvider peekPooled() {
        XaConnectionProvider xaFacade = pooled.peek();
        if (xaFacade == null) {
            xaFacade = providerSupplier.get();
            try {
                xaFacade.open();
            } catch (Exception e) {
                rethrow(e);
            }
            pooled.offer(xaFacade);
        }
        return xaFacade;
    }
}
