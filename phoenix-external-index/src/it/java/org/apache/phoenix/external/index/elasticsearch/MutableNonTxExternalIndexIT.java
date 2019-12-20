package org.apache.phoenix.external.index.elasticsearch;

import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

public class MutableNonTxExternalIndexIT extends BaseExternalIndexIT {
    public MutableNonTxExternalIndexIT(boolean mutable, boolean columnEncoded) {
        super(mutable, columnEncoded);
    }
    @Parameterized.Parameters(name="MutableNonTxExternalIndexIT_mutable={0},transactional={1},columnEncoded={2}")
    // name is used by failsafe as file name in reports
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {
                { true, false }, { true, true }
        });
    }
}
