package com.kinger.hadoop.mapreduce.lib.reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JDBCImportReducer extends Reducer<LongWritable, Text, Text, NullWritable> {

    /**
     * Nothing to do here
     *
     * @param  context The Reducer.Context
     *
     */

    @Override
    public void setup(Context context) {

    }

    /**
     * Take the data from the mappers and de-duplicate.  Then write to the output formatter.
     *
     * @param  key The Md5Hash of the row
     * @param  values iterables of json grouped by the key.
     * @param  context The Reducer.Context
     *
     */

    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{

        context.getCounter("Totals", "Keys").increment(1);

        for (Text value: values) {
            context.getCounter("Totals", "Values").increment(1);
            context.write(value, NullWritable.get());
        }

    }

    /**
     * Nothing to do here
     *
     * @param  context The Reducer.Context
     *
     */

    @Override
    public void cleanup(Context context) {

    }
}
