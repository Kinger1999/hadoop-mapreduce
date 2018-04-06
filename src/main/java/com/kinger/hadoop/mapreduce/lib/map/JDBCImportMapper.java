package com.kinger.hadoop.mapreduce.lib.map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.kinger.hadoop.mapreduce.utils.DAOManager;
import com.kinger.hadoop.mapreduce.utils.MySQLConstants;
import com.kinger.hadoop.mapreduce.utils.JDBCImportConstants;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.Map;

public class JDBCImportMapper extends Mapper<LongWritable, Text, LongWritable, Text> {


    public static final Logger LOG = LoggerFactory.getLogger(JDBCImportMapper.class);
    private String table;
    private String pkey;
    BasicDataSource ds;
    static ObjectMapper mapper;


    /**
     * Sets up the mapper object.  This includes getting info from the configuration object
     * and setting up the database connection
     *
     * @param  context  the mapper context
     */

    @Override
    public void setup(Context context) {

        mapper = new ObjectMapper();

        Configuration conf = context.getConfiguration();
        String url = conf.get(JDBCImportConstants.JDBC_URL);
        String database = conf.get(JDBCImportConstants.DATABASE_NAME);
        table = conf.get(JDBCImportConstants.TABLE_NAME);
        pkey = conf.get(JDBCImportConstants.TABLE_PK);
        String user = conf.get(JDBCImportConstants.USER, MySQLConstants.MYSQL_USER);
        String password = conf.get(JDBCImportConstants.PASSWORD, MySQLConstants.MYSQL_PASS);

        try {

            ds = DAOManager.getDataSource(url, user, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    /**
     * The map function takes the input value (SQL split) runs it against the database, returns the rows, converts them
     * to json and outputs to the intermediate step.
     *
     * @param  key  The row key defined by the input.
     * @param  value The Text value sent to the map function. In this case it's the query split to be run.
     * @param  context The Mapper.Context
     *
     */
    private Connection conn;
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {


        try {

            long start = System.currentTimeMillis();

            conn = ds.getConnection();
            conn.setReadOnly(true);

            Statement st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

            LOG.debug("Connection object type is: " + conn.getClass());
            LOG.debug("Query: " + value.toString());
            LOG.debug("ResultSet Fetch Size: " + st.getFetchSize());

            long rowCount = 0;

            ResultSet rs = st.executeQuery(value.toString());
            ResultSetMetaData rsmd = rs.getMetaData();
            long columnCount = rsmd.getColumnCount();
            mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

            rs.setFetchSize(1000);
            rs.setFetchDirection(ResultSet.FETCH_FORWARD);

            while (rs.next()) {

                try {

                    Map<String, Object> row = new LinkedHashMap<>();

                    rowCount++;

                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = rsmd.getColumnName(i);
                        row.put(columnName, rs.getObject(i));
                    }

                    context.write(key, new Text(mapper.writeValueAsString(row)));
                    //context.write(key, new Text(gson.toJson(row)));

                }
                catch (Exception e) {
                    context.getCounter("Errors", "JSON Failures").increment(1l);
                    LOG.error(e.getMessage());
                }

            }
            rs.close();
            st.close();
            conn.close();

            long end = System.currentTimeMillis();
            long duration = end - start;

            LOG.info(String.format(
                "Found %d records in %d ms. %.3f rows per ms.",
                rowCount, duration, rowCount / (float) duration
            ));

        } catch (SQLException se) {
            context.getCounter("Error", se.getMessage()).increment(1);
            throw new IOException(se);
        }


    }

    /**
     * Clean up all the things such as database connections
     *
     * @param  context The Mapper.Context
     *
     */

    @Override
    public void cleanup(Context context) {
    }

}
