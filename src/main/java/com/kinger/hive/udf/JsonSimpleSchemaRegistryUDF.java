package com.kinger.hive.udf;

import com.kinger.schema.NoValidSchemaException;
import com.kinger.schema.SimpleSchemaRegistry;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JsonSimpleSchemaRegistryUDF extends UDF {

    private static final Logger LOG = LoggerFactory.getLogger(JsonSimpleSchemaRegistryUDF.class);
    private SimpleSchemaRegistry registry;

    public JsonSimpleSchemaRegistryUDF() {
        registry = new SimpleSchemaRegistry();
    }

    public Text evaluate(Text schema) throws IOException, NoValidSchemaException {

        Text result = null;

        result = new Text(registry.get(schema.toString()).asString());

        return result;

    }

}
