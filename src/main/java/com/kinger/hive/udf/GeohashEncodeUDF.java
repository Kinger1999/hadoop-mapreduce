package com.kinger.hive.udf;

import com.github.davidmoten.geo.GeoHash;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class GeohashEncodeUDF extends UDF {


    public Text evaluate(Text inLatitude, Text inLongitude, Text inPrecision) {

        if (inLatitude == null | inLongitude == null | inPrecision == null) {
            return null;
        }

        double latitude = Double.parseDouble(inLatitude.toString());
        double longitude = Double.parseDouble(inLongitude.toString());
        int precision = Integer.parseInt(inPrecision.toString());

        if (precision > GeoHash.MAX_HASH_LENGTH) {
            precision = GeoHash.MAX_HASH_LENGTH;
        }

        return new Text(GeoHash.encodeHash(latitude, longitude, precision));

    }

}
