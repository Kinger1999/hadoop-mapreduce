package com.kinger.hadoop.mapreduce.job;

import com.kinger.hadoop.mapreduce.lib.map.JDBCImportMapper;
import com.kinger.hadoop.mapreduce.lib.partitioner.JDBCEvenPartitioner;
import com.kinger.hadoop.mapreduce.lib.reduce.JDBCImportReducer;
import com.kinger.hadoop.mapreduce.utils.DAOManager;
import com.kinger.hadoop.mapreduce.utils.MySQLConstants;
import com.kinger.hadoop.mapreduce.utils.JDBCImportConstants;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBCImportJob extends Configured implements Tool {

    public static final Logger LOG = LoggerFactory.getLogger(JDBCImportJob.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JDBCImportJob(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();

        conf.setBoolean("fs.s3a.fast.upload", true);

        String url = conf.get(JDBCImportConstants.JDBC_URL);
        String table = conf.get(JDBCImportConstants.TABLE_NAME, null);
        String database = conf.get(JDBCImportConstants.DATABASE_NAME, null);
        String pkey = conf.get(JDBCImportConstants.TABLE_PK, "id");
        long splitSize = conf.getLong(JDBCImportConstants.TABLE_SPLIT_SIZE, 10000);
        String date = conf.get(JDBCImportConstants.PARTITION_DATE);
        int reducers = conf.getInt("mapred.reduce.tasks", 1);
        int threads = conf.getInt("mapred.map.threads", 1);
        int mappers = conf.getInt("mapred.map.tasks", 1);
        String user = conf.get(JDBCImportConstants.USER, MySQLConstants.MYSQL_USER);
        String password = conf.get(JDBCImportConstants.PASSWORD, MySQLConstants.MYSQL_PASS);
        String columns = conf.get(JDBCImportConstants.TABLE_COLUMNS, "*");
        String output = conf.get(JDBCImportConstants.OUTPUT_LOCATION, "/tmp");
        String whereClause = conf.get(JDBCImportConstants.WHERE_CLAUSE, "");
        String outputCompression = conf.get(JDBCImportConstants.OUTPUT_COMPRESSION, "snappy").toLowerCase();
        long inputMinSplitSize = conf.getInt(FileInputFormat.SPLIT_MINSIZE, 0);
        long inputMaxSplitSize = conf.getInt(FileInputFormat.SPLIT_MAXSIZE, 10*1024*1024);
        boolean shuffleSplits = conf.getBoolean(JDBCImportConstants.SHUFFLE_SPLITS, false);

        LOG.info("Mapper Memory MB: " + conf.get(MRJobConfig.MAP_MEMORY_MB));
        LOG.info("Reducer Memory MB: " + conf.get(MRJobConfig.REDUCE_MEMORY_MB));

        // Check if we have the minimum args to run the job

        if (table == null) {
            throw new SQLException("Table cannot be blank");
        }

        if (database == null) {
            throw new SQLException("Database cannot be blank");
        }

        if (pkey == null) {
            throw new SQLException ("Table Split Key cannot be left blank");
        }

        if (url == null) {
            throw new SQLException ("JDBC Url cannot be left blank");
        }

        if (output == null) {
            throw new SQLException ("Output Location cannot be left blank");
        }

        if (user == null) {
            throw new SQLException ("Database User cannot be left blank");
        }

        if (password == null) {
            throw new SQLException ("Database Password cannot be left blank");
        }

        if (date == null) {
            throw new SQLException ("Partition Date cannot be left blank");
        }

        if (whereClause.trim().toLowerCase().startsWith("where")) {
            throw new SQLException("Where statement cannot begin with 'WHERE'.  Try 'AND' ");
        }

        // Set the directory where we want to output the results
        Path outputPath = new Path(output);

        // Since we're using S3, we need to use the FileSystem.get(URI, Configuration)
        FileSystem outputfs = FileSystem.get(outputPath.toUri(), conf);

        // Set the file where we store the query splits to run on the mappers
        String splitFile = String.format(
                "s3://%s/tmp/jdbc_import/%s.splits",
                outputfs.getUri().getHost(),
                table.toString().toLowerCase()
        );

        LOG.info(String.format("Split file: %s", splitFile));

        Path splitPath = new Path(splitFile);

        // Delete the existing split file if it exists, otherwise the job will blow up
        if (outputfs.exists(splitPath))
            outputfs.delete(splitPath, true);

        // Delete the existing output directory otherwise the job will blow up
        if (outputfs.exists(outputPath))
            outputfs.delete(outputPath, true);

        FSDataOutputStream out = outputfs.create(splitPath);

        List<String> splits = getSplits(user, password, url, database, table,
                splitSize, pkey, whereClause, shuffleSplits);

        for (String split : splits) {
            out.write(split.getBytes());
        }

        long fileSize = out.size();
        if (mappers > 1) {
            inputMaxSplitSize = (fileSize / mappers);
        }



        out.close();
        outputfs.close();



        Job job = Job.getInstance(conf);
        job.setJobName(String.format("MySQL Import: %s", table));
        job.setJarByClass(JDBCImportJob.class);
        job.setMapperClass(MultithreadedMapper.class);
        MultithreadedMapper.setMapperClass(job, JDBCImportMapper.class);
        MultithreadedMapper.setNumberOfThreads(job, threads);
        job.setReducerClass(JDBCImportReducer.class);
        job.setPartitionerClass(JDBCEvenPartitioner.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(reducers);

        FileInputFormat.addInputPath(job, splitPath);
        FileInputFormat.setMaxInputSplitSize(job, inputMaxSplitSize);
        FileInputFormat.setMinInputSplitSize(job, inputMinSplitSize);
        FileOutputFormat.setOutputPath(job, outputPath);
        // need to make compression configurable from the command line
        if (outputCompression.equalsIgnoreCase("gzip"))
            FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        if (outputCompression.equalsIgnoreCase("snappy"))
            FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        FileOutputFormat.setCompressOutput(job, true);

        LOG.info("Output Compression used: " + outputCompression);
        LOG.info("Input Min Split Size: " + FileInputFormat.getMinSplitSize(job));
        LOG.info("Input Max Split Size: " + FileInputFormat.getMaxSplitSize(job));
        LOG.info("Input File Size: " + fileSize);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    private static List<String> getSplits(String user, String password, String url, String database, String table,
                                          long split_size, String pkey, String whereClause, boolean shuffle)
            throws SQLException, ClassNotFoundException {

        List<String> splits = new ArrayList<String>();

        String query = String.format(
                "SELECT MIN(%s), MAX(%s) FROM %s.%s",
                pkey, pkey, database, table
        );

        LOG.info("Getting SQL Splits");
        LOG.info("Getting Database Connection");

        BasicDataSource ds = DAOManager.getDataSource(url, user, password);
        Connection conn = ds.getConnection();
        Statement st = conn.createStatement();

        LOG.info(query);

        ResultSet rs = st.executeQuery(query);

        LOG.info("Calculating SQL Splits");

        while (rs.next()) {

            long min = rs.getLong(1);
            long max = rs.getLong(2);

            long splitStart = min;
            long splitEnd = min;

            String split = "";

            while (splitEnd <= max) {

                splitEnd += split_size;

                split = getSplitQuery(database, table, pkey, splitStart, splitEnd);
                split = split.replace("$CONDITIONS", whereClause);

                splits.add(split);

                splitStart = (splitEnd + 1);

            }

            split = getSplitQuery(database, table, pkey, splitStart, splitEnd);
            split = split.replace("$CONDITIONS", whereClause);

            splits.add(split);

        }

        rs.close();
        conn.close();

        LOG.info("Number of SQL splits: " + splits.size());
        if (shuffle)
            Collections.shuffle(splits);
        return splits;

    }

    private static String getSplitQuery(String database, String table, String pkey, long splitStart, long splitEnd) {
        String query =  String.format("SELECT SQL_NO_CACHE * FROM %s.%s WHERE (%s >= %d AND %s < %d) $CONDITIONS\n",
            database, table, pkey, splitStart, pkey, splitEnd);
        return query;
    }

}
