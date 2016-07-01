package com.gaosheng.dataMove;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
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

}
