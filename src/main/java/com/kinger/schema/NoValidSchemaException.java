package com.kinger.schema;

public class NoValidSchemaException extends Exception {

    public NoValidSchemaException(String message) {
        super(message);
    }

    public NoValidSchemaException() {
        super("Schema doesn't exist");
    }
}
