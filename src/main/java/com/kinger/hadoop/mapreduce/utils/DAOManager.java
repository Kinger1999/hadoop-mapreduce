package com.kinger.hadoop.mapreduce.utils;

import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.*;


public class DAOManager {

    private static BasicDataSource dataSource;

    public static BasicDataSource getDataSource(String url, String username, String password)
        throws ClassNotFoundException {

        if (dataSource == null) {

            if (url.contains("jdbc:mysql"))
                Class.forName("com.mysql.jdbc.Driver");

            if (url.contains("jdbc:redshift"))
                Class.forName("com.amazon.redshift.jdbc.Driver");

            if (url.contains("jdbc:postgresql"))
                Class.forName("org.postgresql.Driver");

            BasicDataSource ds = new BasicDataSource();
            ds.setUrl(url);
            ds.setUsername(username);
            ds.setPassword(password);
            ds.setMinIdle(5);
            ds.setMaxIdle(10);
            ds.setMaxOpenPreparedStatements(100);
            dataSource = ds;
        }
        return dataSource;
    }

}
