package com.flavorwocky.migrate;

import com.flavorwocky.exception.MigrateException;
import com.flavorwocky.util.ConnectionFactory;

import java.sql.*;

/**
 * Migration Main
 * Created by luanne on 04/02/14.
 */
public class MigrateDb {
    final static String SUBTYPE_QUERY = "match n where n.__type__={1} return count(n) as total";


    public static void main(String[] args) {
        try (Connection conn = ConnectionFactory.getInstance().getEmbeddedConnection(false)) {


            //Add an Ingredient label to all ingredient nodes
            migrateIngredients(conn);

            //Add a User label to all user nodes
            migrateUsers(conn);

            //Remove the category relation, set it as a property on the ingredient node (migrate later to a label)
             migrateCategories(conn);

            //Add a Pairing label on the pairing nodes
            migratePairings(conn);

            //Add a LatestPairing on the latest pairing nodes
            migrateLatestPairings(conn);

            //Remove sub reference nodes and the root
            removeRootAndReferences(conn);

            conn.commit();
            ConnectionFactory.getInstance().closeEmbeddedConnection();
        } catch (SQLException | MigrateException sqle) {
            System.err.print(sqle);
        }

    }

    private static void migrateIngredients(Connection conn) throws SQLException, MigrateException {
        long existing = 0, modified = 0;

        try (PreparedStatement ps = conn.prepareStatement(SUBTYPE_QUERY)) {
            ps.setString(1, "com.herokuapp.flavorwocky.Ingredient");

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    existing = rs.getLong("total");
                }
            }
        }

        String updateSt = "match n where n.__type__={1} set n :Ingredient";
        try (PreparedStatement ps = conn.prepareStatement(updateSt)) {
            ps.setString(1, "com.herokuapp.flavorwocky.Ingredient");
            ps.executeUpdate();
        }

        try (Statement st = conn.createStatement()) {

            try (ResultSet rs = st.executeQuery("match (n:Ingredient) return count(n) as modified")) {
                if (rs.next()) {
                    modified = rs.getLong("modified");
                }

            }

        }
        if (existing != modified) {
            throw new MigrateException("Ingredients migrate failed. Expected " + existing + " nodes to be updated but " + modified + " were.");
        }
        System.out.println("...Ingredient labels migrated");

    }

    private static void migrateUsers(Connection conn) throws SQLException, MigrateException {
        long existing = 0, modified = 0;

        try (PreparedStatement ps = conn.prepareStatement(SUBTYPE_QUERY)) {
            ps.setString(1, "com.herokuapp.flavorwocky.User");

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    existing = rs.getLong("total");
                }
            }
        }

        String updateSt = "match n where n.__type__={1} set n :User";
        try (PreparedStatement ps = conn.prepareStatement(updateSt)) {
            ps.setString(1, "com.herokuapp.flavorwocky.User");
            ps.executeUpdate();
        }

        try (Statement st = conn.createStatement()) {

            try (ResultSet rs = st.executeQuery("match (n:User) return count(n) as modified")) {
                if (rs.next()) {
                    modified = rs.getLong("modified");
                }

            }

        }
        if (existing != modified) {
            throw new MigrateException("Users migrate failed. Expected " + existing + " nodes to be updated but " + modified + " were.");
        }
        System.out.println("...User labels migrated");

    }

    private static void migrateCategories(Connection conn) throws SQLException, MigrateException {
        long existing = 0, modified = 0;
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("match (n:Ingredient) return count(n) as total")) {
                if (rs.next()) {
                    existing = rs.getLong("total");
                }

            }
        }

        String updateSt = "match c  where c.__type__= {1} match (c)<-[:category]-(i:Ingredient) set i.category=c.name";
        try (PreparedStatement ps = conn.prepareStatement(updateSt)) {
            ps.setString(1, "com.herokuapp.flavorwocky.Category");
            ps.executeQuery();
        }

        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery("match (n:Ingredient) where has(n.category) return count(n) as total")) {
                if (rs.next()) {
                    modified = rs.getLong("total");
                }
            }
        }

        if (existing != modified) {
            throw new MigrateException("Category migrate failed. Expected " + existing + " nodes to be updated but " + modified + " were.");
        }
        System.out.println("...Categories migrated");

        String deleteQuery = "match (c)-[r]-() where c.__type__={1} delete r,c";
        try (PreparedStatement ps = conn.prepareStatement(deleteQuery)) {
            ps.setString(1, "com.herokuapp.flavorwocky.Category");
            ps.executeUpdate();
        }

        long categorySubref = 1;
        try (PreparedStatement ps = conn.prepareStatement(SUBTYPE_QUERY)) {
            ps.setString(1, "com.herokuapp.flavorwocky.Catrgory");

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    categorySubref = rs.getLong("total");
                }
            }
        }
        if (categorySubref > 0) {
            throw new MigrateException("Could not delete the Category subreference");
        }
        System.out.println(".....Category subreference deleted");


    }

    private static void migratePairings(Connection conn) throws SQLException, MigrateException {
        long existing = 0, modified = 0;

        try (PreparedStatement ps = conn.prepareStatement(SUBTYPE_QUERY)) {
            ps.setString(1, "com.herokuapp.flavorwocky.Pairing");

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    existing = rs.getLong("total");
                }
            }
        }

        String updateSt = "match n where n.__type__={1} set n :Pairing";
        try (PreparedStatement ps = conn.prepareStatement(updateSt)) {
            ps.setString(1, "com.herokuapp.flavorwocky.Pairing");
            ps.executeUpdate();
        }

        try (Statement st = conn.createStatement()) {

            try (ResultSet rs = st.executeQuery("match (n:Pairing) return count(n) as modified")) {
                if (rs.next()) {
                    modified = rs.getLong("modified");
                }

            }

        }
        if (existing != modified) {
            throw new MigrateException("Pairing migrate failed. Expected " + existing + " nodes to be updated but " + modified + " were.");
        }
        System.out.println("...Pairing labels migrated");

    }

    private static void migrateLatestPairings(Connection conn) throws SQLException, MigrateException {
        long existing = 0, modified = 0;

        try (PreparedStatement ps = conn.prepareStatement(SUBTYPE_QUERY)) {
            ps.setString(1, "com.herokuapp.flavorwocky.LatestPairing");

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    existing = rs.getLong("total");
                }
            }
        }

        String updateSt = "match n where n.__type__={1} set n :LatestPairing";
        try (PreparedStatement ps = conn.prepareStatement(updateSt)) {
            ps.setString(1, "com.herokuapp.flavorwocky.LatestPairing");
            ps.executeUpdate();
        }

        try (Statement st = conn.createStatement()) {

            try (ResultSet rs = st.executeQuery("match (n:LatestPairing) return count(n) as modified")) {
                if (rs.next()) {
                    modified = rs.getLong("modified");
                }

            }

        }
        if (existing != modified) {
            throw new MigrateException("LatestPairing migrate failed. Expected " + existing + " nodes to be updated but " + modified + " were.");
        }
        System.out.println("...LatestPairing labels migrated");

    }

    private static void removeRootAndReferences(Connection conn) throws SQLException, MigrateException {

        String deleteQuery = "start n=node(0) match n-[:SUBREFERENCE]->(sr) with sr,n match sr-[r]-() delete r,sr,n";
        try (Statement st = conn.createStatement()) {
            st.executeUpdate(deleteQuery);
        }

        System.out.println("...Root and reference nodes removed");

    }
}
