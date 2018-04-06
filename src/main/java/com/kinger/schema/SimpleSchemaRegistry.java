package com.kinger.schema;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class SimpleSchemaRegistry {

    private ClassLoader cl = getClass().getClassLoader();
    private SimpleSchemaRegistryMap registryMap = new SimpleSchemaRegistryMap();
    protected URL classpath = null;

    public SimpleSchemaRegistry() {
    }

    public SimpleSchemaRegistry(String name) throws NoValidSchemaException {
        this.get(name);
    }

    public SimpleSchemaRegistry get(String name) throws NoValidSchemaException {

        if (registryMap.containsKey(name)) {
            classpath = cl.getResource(registryMap.get(name));
        } else {
            throw new NoValidSchemaException(
                "Specified schema does not exist."
            );
        }

        return this;

    }

    public URL asURL() throws IOException {
        if (this.classpath == null) {
            return null;
        }
        return this.classpath;
    }

    public InputStream asInputStream() throws IOException {
        if (this.classpath == null) {
            return null;
        }
        return this.classpath.openStream();
    }

    public String asString() throws IOException {
        if (this.classpath == null) {
            return null;
        }
        return IOUtils.toString(this.classpath.openStream());
    }

}
