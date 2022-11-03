package com.example.sql_server;

import com.example.flag.FeatureFlag;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * FeatureFlagHelper is here to make managing multiple flags easier in tests.
 *
 * <p>Start by adding this class as an @Rule in your spec: `@Rule FeatureFlagHelper flagHelper = new
 * FeatureFlagHelper([Default Flags]);`
 *
 * <p>Then you just need to call `addFlags` and/or `removeFlags` on `flagHelper` at the start of each test method or in
 * an @Before if you want every test to have the set of flags) and this helper will handle actually making that so. It
 * calls `FeatureFlag.setupForTest` every time a change to the set of active flags is changed so you don't have to.
 *
 * <p>FeatureFlagHelper will also automatically reset to a set of default flags at the start of every test.
 */
public class FeatureFlagHelper implements TestRule {
    private final Set<String> activeFlags = new HashSet<>();

    private final Set<String> defaultFlags = new HashSet<>();

    public FeatureFlagHelper() {}

    public FeatureFlagHelper(String... defaultFlags) {
        this.defaultFlags.addAll(Arrays.asList(defaultFlags));
    }

    public Statement apply(final Statement base, final Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                clearFlags();
                base.evaluate();
            }
        };
    }

    public void addFlags(String... flags) {
        activeFlags.addAll(Arrays.asList(flags));
        syncFlags();
    }

    public void removeFlags(String... flags) {
        activeFlags.removeAll(Arrays.asList(flags));
        syncFlags();
    }

    public void clearFlags() {
        activeFlags.clear();
        activeFlags.addAll(defaultFlags);
        syncFlags();
    }

    public void syncFlags() {
        System.out.println("Active Flags: " + activeFlags);
//        FeatureFlag.setupForTest(activeFlags.toArray(new String[0]));
    }
}
