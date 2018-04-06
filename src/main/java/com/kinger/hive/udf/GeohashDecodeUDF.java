package com.kinger.hive.udf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.davidmoten.geo.GeoHash;
import com.github.davidmoten.geo.LatLong;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class GeohashDecodeUDF extends UDF {

    ObjectMapper mapper = new ObjectMapper();

    public Text evaluate(Text geohash) throws JsonProcessingException {
        if (geohash == null) {
            return null;
        }

        LatLong location = GeoHash.decodeHash(geohash.toString());

        ObjectNode node = mapper.createObjectNode();
        node.put("latitude", location.getLat());
        node.put("longitude", location.getLon());

        return new Text(mapper.writeValueAsString(node));

    }

}
