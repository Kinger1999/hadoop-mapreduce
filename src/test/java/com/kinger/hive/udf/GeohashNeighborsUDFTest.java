package com.kinger.hive.udf;

import org.apache.hadoop.io.Text;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.List;

public class GeohashNeighborsUDFTest {

    @Test
    public void gehoashNeighborsReturnsList() {

        GeohashNeighborsUDF udf = new GeohashNeighborsUDF();
        List<String> results = udf.evaluate(new Text("9q5c"));
        assertTrue(results instanceof List);

    }

    @Test
    public void gehoashNeighborsReturnsListLength() {

        GeohashNeighborsUDF udf = new GeohashNeighborsUDF();
        List<String> results = udf.evaluate(new Text("9q5c"));
        assertTrue(results.size() == 8);

    }

    @Test
    public void geohashNeighoborsReturnsExpected() {

        // Expected output is [9q4, 9qh, 9q7, 9mg, 9q6, 9mf, 9qk, 9mu]

        String[] expected = "9q4,9qh,9q7,9mg,9q6,9mf,9qk,9mu".split(",");

        GeohashNeighborsUDF udf = new GeohashNeighborsUDF();
        List<String> results = udf.evaluate(new Text("9q5"));

        for (String item : expected) {
            assertTrue(results.contains(item));
        }

    }


}
