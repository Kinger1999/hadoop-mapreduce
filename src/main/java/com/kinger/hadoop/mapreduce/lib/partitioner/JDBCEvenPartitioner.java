package com.kinger.hadoop.mapreduce.lib.partitioner;

import com.kinger.hadoop.mapreduce.lib.map.JDBCImportMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class JDBCEvenPartitioner<K,V> extends Partitioner<K,V> {

    public static final Logger LOG = LoggerFactory.getLogger(JDBCEvenPartitioner.class);
    Random random = new Random();

    @Override
    public int getPartition(K key, V value, int numReducers) {
        if ( numReducers == 0)
            return 0;
        else
            return random.nextInt(numReducers);
    }
}
