package com.example.oracle;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 7/15/2021<br/>
 * Time: 10:26 PM<br/>
 * To change this template use File | Settings | File Templates.
 */

import java.util.Objects;
import java.util.Optional;

/** */
public class OracleSetupTester implements AutoCloseable {

    private OracleApi api;

    public OracleSetupTester(OracleApi api) {
        this.api = Objects.requireNonNull(api);
    }

    public Optional<String> test() {

        try {
            api.checkPermissions();
        } catch (Exception e) {
            return Optional.of(e.getMessage());
        }

        return Optional.empty();
    }

    @Override
    public void close() {
        api.close();
    }
}
