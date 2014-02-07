package com.flavorwocky.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Utility class for JDBC connection management.
 * For simplicity, both server and embedded connections are created from this class.
 * <p/>
 * Created by luanne on 02/02/14.
 */
public class ConnectionFactory {

    private static Properties props;
    private final static ConnectionFactory instance = new ConnectionFactory();
    private static Connection embeddedConnection = null;
    private static Connection serverConnection = null;

    private final static String USERNAME_KEY = "neo4j.server.username";
    private final static String PASSWORD_KEY = "neo4j.server.password";
    private final static String HOST_KEY = "neo4j.server.host";
    private final static String PORT_KEY = "neo4j.server.port";
    private final static String EMBEDDED_PATH_KEY = "neo4j.embedded.path";


    public static ConnectionFactory getInstance() {
        return instance;
    }

    private ConnectionFactory() {
        props = new Properties();
     //   try (InputStream is = getClass().getResourceAsStream("db.properties")) {
        try (InputStream is =  new FileInputStream("config/db.properties")) {
            props.load(is);
        } catch (IOException e) {
            throw new RuntimeException("Could not load properties");
        }

    }

    /**
     * Get a JDBC connection to a Neo4j Server
     * @return Connection
     * @throws SQLException if the connection couldn't be established
     */
    public Connection getServerConnection() throws SQLException {
        if (serverConnection == null) {
            Properties connectionProps = new Properties();
            if (props.getProperty(USERNAME_KEY) != null) {
                connectionProps.put("user", props.getProperty(USERNAME_KEY));
            }
            if (props.getProperty(PASSWORD_KEY) != null) {
                connectionProps.put("password", props.getProperty(PASSWORD_KEY));
            }
            serverConnection = DriverManager.getConnection("jdbc:neo4j://" + props.getProperty(HOST_KEY) + ":" + props.getProperty(PORT_KEY) + "/", connectionProps);

        }
        return serverConnection;
    }

    /**
     * Get a JDBC connection to an embedded database
     * @param autoCommit autoCommit setting
     * @return Connection
     * @throws SQLException if a Connection could not be established
     */
    public Connection getEmbeddedConnection(boolean autoCommit) throws SQLException {
        if (embeddedConnection == null) {
            Properties connectionProps = new Properties();
            embeddedConnection = DriverManager.getConnection("jdbc:neo4j:file:" + props.getProperty(EMBEDDED_PATH_KEY), connectionProps);
            embeddedConnection.setAutoCommit(autoCommit);
        }
        return embeddedConnection;
    }

    /**
     * Close the connection to the Neo4j Server
     * @throws SQLException if something went wrong
     */
    public void closeServerConnection() throws SQLException {
        if(serverConnection!=null) {
            serverConnection.close();
        }
    }

    /**
     * Close the connection to the embedded database
     * @throws SQLException if something went wrong
     */
    public void closeEmbeddedConnection() throws SQLException {
        if(embeddedConnection!=null) {
            embeddedConnection.close();
        }
    }

}
