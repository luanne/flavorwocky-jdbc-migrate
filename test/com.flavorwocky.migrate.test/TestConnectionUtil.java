package com.flavorwocky.migrate.test;

import com.flavorwocky.util.ConnectionFactory;
import junit.framework.Assert;
import org.junit.Test;

import java.sql.SQLException;

/**
 * Created by luanne on 03/02/14.
 */
public class TestConnectionUtil {

    @Test
    public void testServerConnection() {
        try {
            Assert.assertNotNull(ConnectionFactory.getInstance().getServerConnection());
            ConnectionFactory.getInstance().closeServerConnection();
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testEmbeddedConnection() {
        try {
            Assert.assertNotNull(ConnectionFactory.getInstance().getEmbeddedConnection(true));
            ConnectionFactory.getInstance().closeEmbeddedConnection();
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }

}
