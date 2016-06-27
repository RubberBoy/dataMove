package com.gaosheng.dataMove;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class Connect {

    public static Connection getConnection(String propertiesFile) {
        Connection conn = null;
        Properties props = new Properties();
        InputStream is = null;

        try {
            is = Connect.class.getResourceAsStream("/"+propertiesFile);
            props.load(is);
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            StringBuffer jdbcURLString = new StringBuffer();
            jdbcURLString.append("jdbc:oracle:thin:@");
            jdbcURLString.append(props.getProperty("host"));
            jdbcURLString.append(":");
            jdbcURLString.append(props.getProperty("port"));
            jdbcURLString.append(":");
            jdbcURLString.append(props.getProperty("database"));
            conn = DriverManager.getConnection(jdbcURLString.toString(), props
                    .getProperty("user"), props.getProperty("password"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return conn;
    }

    public static int close(Connection conn) {
        try {
            conn.commit();
            System.out.println("数据写入完毕");
            conn.close();
            return 1;
        } catch (SQLException e) {
            return 0;
        }
    }

    public static int executeSql (Connection conn,String sql){
        Statement stat = null;
        try {
            stat = conn.createStatement();
            return stat.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            return 0;
        }finally{
            try {
                stat.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
