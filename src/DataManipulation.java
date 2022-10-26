import java.sql.SQLException;

public interface DataManipulation {

//    public String allContinentNames();
//    public String continentsWithCountryCount();
//    public String FullInformationOfMoviesRuntime(int min, int max);
//    public String findMovieById(int id);

    public int addOneRecord(Records type, String str) throws SQLException;
    public int addFullRecords(String str) throws SQLException;

    void getConnection();
    void closeConnection();
}
