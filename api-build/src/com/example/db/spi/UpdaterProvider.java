package com.example.db.spi;

import com.example.core2.ConnectionParameters;

/**
 * This interface is necessary for service classes belonging to connectors that use the DbIntegrationTests class (i.e.
 * Oracle connector) in order to remove all dependence on the DataUpdater interface.
 *
 * <p>If DbIntegrationTests is rewritten in such a way as to not require the newUpdater method, or if the connectors
 * using that class in tests change to something else, then this interface can be deleted from the code base.
 *
 * @param <T> The updater type
 * @param <C> Credentials (i.e. DbCredentials)
 * @param <S> connector state
 */
public interface UpdaterProvider<T, C, S> {

    T newUpdater(C credentials, S state, ConnectionParameters parameters);
}
