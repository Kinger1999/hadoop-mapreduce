package com.kinger.hive.udf;

import com.kinger.schema.NoValidSchemaException;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class JsonSchemaRegistryUDFTest {

    private Text validSchemaName = new Text("product-ingest-schema");
    private Text invalidSchemaName = new Text("invalid-schema");

    @Test
    public void validSchemaReturnTextTest() {
        try {
            JsonSimpleSchemaRegistryUDF registry = new JsonSimpleSchemaRegistryUDF();
            Text result = registry.evaluate(validSchemaName);
            assertNotNull(result);
        } catch (NoValidSchemaException e) {
            assertTrue(false);
        } catch (IOException e) {
            assertTrue(false);
        }
    }

    @Test
    public void invalidSchemaReturnsNoValidSchemaExceptionTest() {
        try {
            JsonSimpleSchemaRegistryUDF registry = new JsonSimpleSchemaRegistryUDF();
            Text result = registry.evaluate(invalidSchemaName);
            assertNull(result);
        } catch (NoValidSchemaException e) {
            assertTrue(true);
        } catch (IOException e) {
            assertTrue(false);
        }
    }

    @Test
    public void validReturnTypeAsTextTest() {
        try {
            JsonSimpleSchemaRegistryUDF registry = new JsonSimpleSchemaRegistryUDF();
            Text result = registry.evaluate(validSchemaName);
            assertTrue(result instanceof Text);
        } catch (NoValidSchemaException e) {
            assertTrue(false);
        } catch (IOException e) {
            assertTrue(false);
        }
    }





}
