package com.kinger.hive.udf;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    GeohashEncodeUDFTest.class,
    GeohashDecodeUDFTest.class,
    GeohashNeighborsUDFTest.class
})

public class GeohashUDFTestSuite {
    // the class remains empty,
    // used only as a holder for the above annotations
}
