/**
 * This class represents the main entry point for the application.
 * It establishes a connection to the Snowflake database.
 */
package org.apache.flink.connector.jdbc;

import org.apache.flink.connector.jdbc.datasource.connections.SimpleJdbcConnectionProvider;

import java.sql.Connection;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) {
        JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:snowflake://UY98093.snowflakecomputing.com/?db=CYBERSYN&schema=BRONZE")
                .withDriverName("net.snowflake.client.jdbc.SnowflakeDriver")
                .withUsername("ECATERINAL")
                .withPassword("DJiglok678!")
                .build();

        SimpleJdbcConnectionProvider connectionProvider = new SimpleJdbcConnectionProvider(jdbcOptions);
        try {
            Connection connection = connectionProvider.getOrEstablishConnection();
            System.out.println("Connection to Snowflake established successfully!");
            // You can add more code here to interact with your Snowflake database
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
