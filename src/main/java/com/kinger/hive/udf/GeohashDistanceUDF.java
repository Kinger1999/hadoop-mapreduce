package com.kinger.hive.udf;

import com.github.davidmoten.geo.GeoHash;
import com.github.davidmoten.geo.LatLong;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class GeohashDistanceUDF extends UDF {

    public double evaluate(Text hash1, Text hash2, Text unit) {

            LatLong point1 = GeoHash.decodeHash(hash1.toString());
            LatLong point2 = GeoHash.decodeHash(hash2.toString());

            double lat1 = point1.getLat();
            double lat2 = point2.getLat();
            double lon1 = point1.getLon();
            double lon2 = point2.getLon();

            double theta = lon1 - lon2;
            double dist = Math.sin(deg2rad(lat1))
                    * Math.sin(deg2rad(lat2))
                    + Math.cos(deg2rad(lat1))
                    * Math.cos(deg2rad(lat2))
                    * Math.cos(deg2rad(theta));
            dist = Math.acos(dist);
            dist = rad2deg(dist);
            dist = dist * 60 * 1.1515;
            if (unit.toString().equalsIgnoreCase("K")) {
                dist = dist * 1.609344;
            } else if (unit.toString().equalsIgnoreCase("N")) {
                dist = dist * 0.8684;
            }

            return dist;

    }

    public double evaluate(Text hash1, Text hash2) {
        return evaluate(hash1, hash2, new Text("M"));
    }

    private static double deg2rad(double deg) {
        return (deg * Math.PI / 180.0);
    }

    private static double rad2deg(double rad) {
        return (rad * 180 / Math.PI);
    }
}
