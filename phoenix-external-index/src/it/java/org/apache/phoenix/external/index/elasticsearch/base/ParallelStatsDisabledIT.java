package org.apache.phoenix.external.index.elasticsearch.base;

import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

/**
 * copy of phoenix-core module
 */
@Category(ParallelStatsDisabledTest.class)
public abstract class ParallelStatsDisabledIT extends BaseTest {

    @BeforeClass
    public static final void doSetup() throws Exception {
        setUpTestDriver(ReadOnlyProps.EMPTY_PROPS);
    }

    @AfterClass
    public static void tearDownMiniCluster() throws Exception {
        BaseTest.tearDownMiniClusterIfBeyondThreshold();
    }
}
