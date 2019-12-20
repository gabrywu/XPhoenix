package org.apache.phoenix.external.index.elasticsearch;

import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

public class ImmutableNonTxExternalIndexIT extends BaseExternalIndexIT {
    public ImmutableNonTxExternalIndexIT(boolean mutable, boolean columnEncoded) {
        super(mutable, columnEncoded);
    }
    @Parameterized.Parameters(name="ImmutableNonTxExternalIndexIT_mutable={0},columnEncoded={1}")
    // name is used by failsafe as file name in reports
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {
                { false, false }, { false, true }
        });
    }
}
