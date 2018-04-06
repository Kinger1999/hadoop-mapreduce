package com.kinger.hive.udf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.davidmoten.geo.GeoHash;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class GeohashEncodeUDFTest {

    ObjectMapper mapper = new ObjectMapper();
    Text latitude = new Text("67.5");
    Text longitude = new Text("157.5");
    Text precision = new Text("12");

    @Test
    public void testGeohashEncodeReturnText() {

        GeohashEncodeUDF udf = new GeohashEncodeUDF();
        Text hash = udf.evaluate(latitude, longitude, precision);
        assertTrue(hash.toString().equalsIgnoreCase("zs0000000000"));
    }


    @Test
    public void testGeohashEncodeReturnSpecifiedLength() {

        GeohashEncodeUDF udf = new GeohashEncodeUDF();
        Text hash = udf.evaluate(latitude, longitude, precision);
        assertTrue(hash.getLength() == 12);
    }

    @Test
    public void testGeohashEncodeReturnsFour() {

        GeohashEncodeUDF udf = new GeohashEncodeUDF();
        Text hash = udf.evaluate(latitude, longitude, new Text("4"));

        assertTrue(hash.getLength() == 4);
    }

    @Test
    public void testGeohashEncodeReturnsMaxLength() {

        GeohashEncodeUDF udf = new GeohashEncodeUDF();
        Text hash = udf.evaluate(latitude, longitude, new Text("14"));
        assertTrue(hash.getLength() == GeoHash.DEFAULT_MAX_HASHES);
    }

}
