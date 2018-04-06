package com.kinger.hive.udf;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;


import java.io.InputStream;

import static org.junit.Assert.*;


public class JsonSchemaValidatorUDFTest {

    private final ClassLoader cl = getClass().getClassLoader();

    @Test
    public void simpleValidDataTestReturnTrue() {

        String schema = "{}";
        String input = "{}";

        JsonSchemaValidatorUDF udf = new JsonSchemaValidatorUDF();
        BooleanWritable result = udf.evaluate(new Text(schema), new Text(input));
        assertTrue(result.get());

    }

    @Test
    public void validJsonSchemaTestReturnTrue() {

        InputStream schema = cl.getResourceAsStream("schemas/product-ingest-schema.json");
        InputStream input = cl.getResourceAsStream("data/product-ingest-valid-data.json");

        try {

            JsonSchemaValidatorUDF udf = new JsonSchemaValidatorUDF();
            BooleanWritable result = udf.evaluate(
                    new Text(IOUtils.toString(schema)),
                    new Text(IOUtils.toString(input))
            );
            assertTrue(result.get());

        } catch (Exception ioe) {
            // bail out if there's a problem getting the resource files
            assertTrue(false);
        }
    }

    @Test
    public void invalidJsonSchemaTestReturnFalse() {

        InputStream schema = cl.getResourceAsStream("schemas/product-ingest-schema.json");
        InputStream input = cl.getResourceAsStream("data/product-ingest-invalid-data.json");

        try {


            JsonSchemaValidatorUDF udf = new JsonSchemaValidatorUDF();
            BooleanWritable result = udf.evaluate(
                    new Text(IOUtils.toString(schema)),
                    new Text(IOUtils.toString(input))
            );
            assertFalse(result.get());

        } catch (Exception ioe) {
            // bail out if there's a problem getting the resource files
            assertTrue(false);
        }
    }
}
