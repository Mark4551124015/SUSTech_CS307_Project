import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JDBC {
    private final String host = "localhost";
    private final String dbname = "project1";
    private final String user = "root";
    private final String pwd = "";
    private final String port = "3306";

/*    public Connection getNewCon() {
        Connection con = null;
        try {
            Class.forName("org.postgresql.Driver");

        } catch (Exception e) {
            System.err.println("Cannot find the PostgreSQL driver. Check CLASSPATH.");
            System.exit(1);
        }

        try {
            String url = "jdbc:postgresql://" + host + ":" + port + "/" + dbname+"?reWriteBatchedInserts=true";
            con = DriverManager.getConnection(url, user, pwd);

        } catch (SQLException e) {
            System.err.println("Database connection failed");
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return con;
    }*/

    /**
     * MySQL
     * @return
     */
    public Connection getNewCon() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");

        } catch (Exception e) {
            System.err.println("Cannot find the PostgreSQL driver. Check CLASSPATH.");
            System.exit(1);
        }

        try {
            String url = "jdbc:mysql://" + host + ":" + port + "/" + dbname+"?reWriteBatchedInserts=true";
            con = DriverManager.getConnection(url, user, pwd);

        } catch (SQLException e) {
            System.err.println("Database connection failed");
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return con;
    }

/*    *//**
     * SQLite Connect
     * @return
     *//*
    public Connection getNewCon() {
        Connection con = null;
        try {
            Class.forName("org.sqlite.JDBC");

        } catch (Exception e) {
            System.err.println("Cannot find the SQLite driver. Check CLASSPATH.");
            System.exit(1);
        }

        try {
            String url = "jdbc:sqlite:" + sqliteDB;
            con = DriverManager.getConnection(url);

        } catch (SQLException e) {
            System.err.println("Database connection failed");
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return con;
    }*/

}
