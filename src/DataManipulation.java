import java.sql.SQLException;

public interface DataManipulation {

//    public String allContinentNames();
//    public String continentsWithCountryCount();
//    public String FullInformationOfMoviesRuntime(int min, int max);
//    public String findMovieById(int id);

    long addOneRecord(Records type, String str) throws SQLException;
    long addFullRecords(String str) throws SQLException;

    void getConnection();
    void closeConnection();
}
