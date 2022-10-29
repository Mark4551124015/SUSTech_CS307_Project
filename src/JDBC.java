import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBC {
    private final String host = "localhost";
    private final String dbname = "sustc";
    private final String user = "mark455";
    private final String pwd = "314159";
    private final String port = "5432";

    public Connection getNewCon() {
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
    }

}
