package org.apache.phoenix.external.index.elasticsearch;

import org.apache.phoenix.external.index.elasticsearch.util.ParallelParameterized;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * 根据参数并行测试
 */
@RunWith(ParallelParameterized.class)
public class ParallelBaseExternalIndexIT extends BaseExternalIndexIT {
    public ParallelBaseExternalIndexIT(boolean mutable, boolean columnEncoded) {
        super(mutable, columnEncoded);
    }
    @Parameterized.Parameters(name="ParallelBaseExternalIndexIT_mutable={0},columnEncoded={1}")
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {
                { false, false },
                { false, true },
                { true, false },
                { true, true }
        });
    }
}
