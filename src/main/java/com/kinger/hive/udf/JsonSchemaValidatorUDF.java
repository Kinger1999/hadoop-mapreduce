package com.kinger.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(
    name="JsonSchemaValidator",
    value="_FUNC_(String jsonSchema, String jsonInput) : Compare jsonInput to jsonSchema for validation"
)

public class JsonSchemaValidatorUDF extends UDF {

    public static final Logger LOG = LoggerFactory.getLogger(JsonSchemaValidatorUDF.class);

    public BooleanWritable evaluate(Text jsonSchema, Text jsonInput) {

        try {

            JSONObject rawInput = new JSONObject(new JSONTokener(jsonInput.toString()));
            JSONObject rawSchema = new JSONObject(new JSONTokener(jsonSchema.toString()));
            Schema schema = SchemaLoader.load(rawSchema);
            schema.validate(rawInput);
            return new BooleanWritable(true);

        } catch (ValidationException ve) {
            // a schema to data mismatch was found.
            // log the error and return false
            LOG.error(ve.getMessage());
            return new BooleanWritable(false);
        }

    }

}
