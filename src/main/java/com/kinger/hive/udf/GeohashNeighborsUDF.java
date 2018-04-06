package com.kinger.hive.udf;

import com.github.davidmoten.geo.GeoHash;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.List;

public class GeohashNeighborsUDF extends UDF {


    public List<String> evaluate(Text geohash) {

        if (geohash == null) {
            return null;
        }

        return GeoHash.neighbours(geohash.toString());

    }

    public List<String> evaluate(Text geohash, boolean includeHome) {
        List<String> neighborhood = evaluate(geohash);
        neighborhood.add(geohash.toString());
        return neighborhood;

    }


}
