package com.kinger.hive.udf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class GeohashDecodeUDFTest {

    ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testGeohashDecodeReturnLatitudeLongitude() {
        try {
            GeohashDecodeUDF udf = new GeohashDecodeUDF();
            Text result = udf.evaluate(new Text("a"));
            ObjectNode node = (ObjectNode) mapper.readTree(result.toString());
            assertTrue(node.get("latitude").asDouble() == 67.5);
            assertTrue(node.get("longitude").asDouble() == 157.5);
        } catch (JsonProcessingException jpe) {
            jpe.printStackTrace();
            assertTrue(false);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testGeohashDecodeReturnText() {
        try {
            GeohashDecodeUDF udf = new GeohashDecodeUDF();
            Text result = udf.evaluate(new Text("a"));
            ObjectNode node = (ObjectNode) mapper.readTree(result.toString());
            assertTrue(result instanceof Text);
        } catch (JsonProcessingException jpe) {
            jpe.printStackTrace();
            assertTrue(false);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testGeohashDecodeReturnJson() {
        try {
            GeohashDecodeUDF udf = new GeohashDecodeUDF();
            Text result = udf.evaluate(new Text("a"));
            ObjectNode node = (ObjectNode) mapper.readTree(result.toString());
        } catch (JsonProcessingException jpe) {
            jpe.printStackTrace();
            assertTrue(false);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            assertTrue(false);
        }
    }


}
