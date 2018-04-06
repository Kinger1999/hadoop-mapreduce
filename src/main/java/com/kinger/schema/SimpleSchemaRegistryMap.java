package com.kinger.schema;

import java.util.HashMap;
import java.util.Map;

public class SimpleSchemaRegistryMap {

    private final Map<String,String> registry = new HashMap<>();

    public SimpleSchemaRegistryMap() {
        registry.put("product-ingest-schema", "schemas/product-ingest-schema.json");
    }

    public String get(String schema) {
        return registry.get(schema);
    }

    public boolean containsKey(String name) {
        return registry.containsKey(name);
    }


}
