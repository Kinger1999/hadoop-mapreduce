package com.kinger.hive.udf;

import com.kinger.schema.NoValidSchemaException;
import com.kinger.schema.SimpleSchemaRegistry;
import com.kinger.schema.SimpleSchemaRegistryMap;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class SimpleSchemaRegistryTest {

    private String schemaName = "product-ingest-schema";

    @Test
    public void emptyConstructorReturnsNoValidSchemaException() {
        try {
            SimpleSchemaRegistry registry = new SimpleSchemaRegistry();
            String schema = registry.get("").asString();
            assertTrue(false);
        } catch (IOException ioe) {
            assertTrue(false);
        } catch (NoValidSchemaException e) {
            assertTrue(true);
        }
    }

    @Test
    public void validConstructorDoesNotReturnNoValidSchemaException() {
        try {
            SimpleSchemaRegistry registry = new SimpleSchemaRegistry();
            String schema = registry.get(schemaName).asString();
            assertTrue(true);
        } catch (IOException ioe) {
            assertTrue(false);
        } catch (NoValidSchemaException e) {
            assertTrue(false);
        }
    }

    @Test
    public void returnConstructorSpecifiedSchemaAsStringTest() {

        try {
            SimpleSchemaRegistry registry = new SimpleSchemaRegistry(schemaName);
            String schema = registry.asString();
            assertTrue(schema instanceof String);
        } catch (IOException ioe) {
            assertTrue(false);
        } catch (NoValidSchemaException e) {
            assertTrue(false);
        }

    }

    @Test
    public void returnConstructorSpecifiedSchemaAsURLTest() {

        try {
            SimpleSchemaRegistry registry = new SimpleSchemaRegistry(schemaName);
            URL schema = registry.asURL();
            assertTrue(schema instanceof URL);
        } catch (IOException ioe) {
            assertTrue(false);
        } catch (NoValidSchemaException e) {
            assertTrue(false);
        }

    }

    @Test
    public void returnConstructorSpecifiedSchemaAsInputStreamTest() {

        try {
            SimpleSchemaRegistry registry = new SimpleSchemaRegistry(schemaName);
            InputStream schema = registry.asInputStream();
            assertTrue(schema instanceof InputStream);
        } catch (IOException ioe) {
            assertTrue(false);
        } catch (NoValidSchemaException e) {
            assertTrue(false);
        }

    }

    @Test
    public void returnMethodSpecifiedSchemaAsStringTest() {

        try {
            SimpleSchemaRegistry registry = new SimpleSchemaRegistry();
            String schema = registry.get(schemaName).asString();
            assertTrue(schema instanceof String);
        } catch (IOException ioe) {
            assertTrue(false);
        } catch (NoValidSchemaException e) {
            assertTrue(false);
        }

    }

    @Test
    public void returnMethodSpecifiedSchemaAsURLTest() {

        try {
            SimpleSchemaRegistry registry = new SimpleSchemaRegistry();
            URL schema = registry.get(schemaName).asURL();
            assertTrue(schema instanceof URL);
        } catch (IOException ioe) {
            assertTrue(false);
        } catch (NoValidSchemaException e) {
            assertTrue(false);
        }

    }

    @Test
    public void returnMethodSpecifiedSchemaAsInputStreamTest() {

        try {
            SimpleSchemaRegistry registry = new SimpleSchemaRegistry();
            InputStream schema = registry.get(schemaName).asInputStream();
            assertTrue(schema instanceof InputStream);
        } catch (IOException ioe) {
            assertTrue(false);
        } catch (NoValidSchemaException e) {
            assertTrue(false);
        }

    }
    @Test
    public void emptySchemaNameAsURLReturnsNullTest() {

        try {
            SimpleSchemaRegistry registry = new SimpleSchemaRegistry();
            assertNull(registry.asURL());
        } catch (IOException ioe) {
            assertTrue(false);
        }

    }

    @Test
    public void emptySchemaNameAsStringReturnsNullTest() {

        try {
            SimpleSchemaRegistry registry = new SimpleSchemaRegistry();
            assertNull(registry.asString());
        } catch (IOException ioe) {
            assertTrue(false);
        }

    }

    @Test
    public void emptySchemaNameAsInputStreamReturnsNullTest() {

        try {
            SimpleSchemaRegistry registry = new SimpleSchemaRegistry();
            assertNull(registry.asInputStream());
        } catch (IOException ioe) {
            assertTrue(false);
        }

    }
}
