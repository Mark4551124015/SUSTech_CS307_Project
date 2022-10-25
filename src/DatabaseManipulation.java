
import java.sql.*;
import java.time.LocalDate;

public class DatabaseManipulation implements DataManipulation {
    private Connection con = null;
    private ResultSet resultSet;
    private String host = "localhost";
    private String dbname = "sustc";
    private String user = "mark455";
    private String pwd = "314159";
    private String port = "5432";
    private void getConnection() {
        try {
            Class.forName("org.postgresql.Driver");

        } catch (Exception e) {
            System.err.println("Cannot find the PostgreSQL driver. Check CLASSPATH.");
            System.exit(1);
        }

        try {
            String url = "jdbc:postgresql://" + host + ":" + port + "/" + dbname;
            con = DriverManager.getConnection(url, user, pwd);

        } catch (SQLException e) {
            System.err.println("Database connection failed");
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
    private void closeConnection() {
        if (con != null) {
            try {
                con.close();
                con = null;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    /*  as example
    @Override
    public int addOneMovie(String str) {
        getConnection();
        int result = 0;
        String sql = "insert into movies (title, country,year_released,runtime) " +
                "values (?,?,?,?)";
        String movieInfo[] = str.split(";");
        try {
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setString(1, movieInfo[0]);
            preparedStatement.setString(2, movieInfo[1]);
            preparedStatement.setInt(3, Integer.parseInt(movieInfo[2]));
            preparedStatement.setInt(4, Integer.parseInt(movieInfo[3]));
            System.out.println(preparedStatement.toString());
            result = preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
        return result;
    }
    */

    @Override
    public int addOneCompany(String str) {
        getConnection();
        int result = 0;
        String sql = "insert into company (company_id, name)" +
                "values (?,?)";
        String companyInfo[] = str.split(";");
        try {
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setInt(1, Integer.parseInt(companyInfo[0]));
            preparedStatement.setString(2, companyInfo[1]);
            System.out.println(preparedStatement.toString());
            result = preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
        return result;
    }
    @Override
    public int addOneRecord(Records type, String str) {
        getConnection();
        int result = 0;
        String sql;
        switch (type) {
            case city -> {
                sql = "insert into city (city_id, name)" +
                        "values (?,?)";
                String[] Info = str.split(";");
                try {
                    //Insertion
                    PreparedStatement preparedStatement = con.prepareStatement(sql);

                    preparedStatement.setInt(1, Integer.parseInt(Info[0]));
                    preparedStatement.setString(2, Info[1]);

                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            }
            case portCity -> {
                sql = "insert into portcity (port_city_id, name)" +
                        "values (?,?)";
                String[] Info = str.split(";");
                try {
                    PreparedStatement preparedStatement = con.prepareStatement(sql);

                    preparedStatement.setInt(1, Integer.parseInt(Info[0]));
                    preparedStatement.setString(2, Info[1]);

                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            }
            case company -> {
                sql = "insert into company (company_id, name)" +
                        "values (?,?)";
                String[] Info = str.split(";");
                try {
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setInt(1, Integer.parseInt(Info[0]));
                    preparedStatement.setString(2, Info[1]);
                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            }
            case container -> {
                sql = "insert into container (container_id, code, type)" +
                        "values (?,?,?)";
                String[] Info = str.split(";");
                try {
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    //Insertion
                    preparedStatement.setInt(1, Integer.parseInt(Info[0]));
                    preparedStatement.setString(2, Info[1]);
                    preparedStatement.setString(3, Info[2]);
                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            }
            case courier -> {
                sql = "insert into courier (courier_id, name, gender, birthday, phone_number, company_id)" +
                        "values (?,?,?,?,?,?)";
                String[] Info = str.split(";");
                try {
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    //Insertion
                    preparedStatement.setInt(1, Integer.parseInt(Info[0]));
                    preparedStatement.setString(2, Info[1]);
                    preparedStatement.setString(3, Info[2]);
                    preparedStatement.setDate(4, Date.valueOf(Info[3]));
                    preparedStatement.setInt(5, Integer.parseInt(Info[4]));
                    preparedStatement.setInt(6, Integer.parseInt(Info[5]));
                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            }
            case ship -> {
                sql = "insert into ship (ship_id, name, company_id)" +
                        "values (?,?,?)";
                String[] Info = str.split(";");
                try {
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    //Insertion
                    preparedStatement.setInt(1, Integer.parseInt(Info[0]));
                    preparedStatement.setString(2, Info[1]);
                    preparedStatement.setInt(3, Integer.parseInt(Info[2]));
                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            }
            case delivery_retrieval -> {
                sql = "insert into delivery_retrieval (dr_id, courier_id, type, date)" +
                        "values (?,?,?,?)";
                String[] Info = str.split(";");
                try {
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    //Insertion
                    preparedStatement.setInt(1, Integer.parseInt(Info[0]));
                    preparedStatement.setInt(2, Integer.parseInt(Info[1]));
                    preparedStatement.setString(3, Info[2]);
                    preparedStatement.setDate(4, Date.valueOf(Info[3]));

                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            }
            case import_export_detail -> {
                sql = "insert into import_export_detail (port_id, type, city_id, tax, date)" +
                        "values (?,?,?,?,?)";
                String[] Info = str.split(";");
                try {
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    //Insertion
                    preparedStatement.setInt(1, Integer.parseInt(Info[0]));
                    preparedStatement.setString(2, Info[1]);
                    preparedStatement.setInt(3, Integer.parseInt(Info[2]));
                    preparedStatement.setInt(4, Integer.parseInt(Info[3]));
                    preparedStatement.setDate(5, Date.valueOf(Info[4]));

                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            }
            case ship_detail -> {
                sql = "insert into ship_detail (ship_id, container_id)" +
                        "values (?,?)";
                String[] Info = str.split(";");
                try {
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    //Insertion
                    preparedStatement.setInt(1, Integer.parseInt(Info[0]));
                    preparedStatement.setInt(2, Integer.parseInt(Info[1]));
                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            }
            case shipping -> {
                sql = "insert into shipping (shipping_id, retrieval_id, export_id, ship_id, container_id, import_id, delivery_id)" +
                        "values (?,?,?,?,?,?,?)";
                String[] Info = str.split(";");
                try {
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    //Insertion
                    preparedStatement.setInt(1, Integer.parseInt(Info[0]));
                    preparedStatement.setInt(2, Integer.parseInt(Info[1]));
                    preparedStatement.setInt(3, Integer.parseInt(Info[2]));
                    preparedStatement.setInt(4, Integer.parseInt(Info[3]));
                    preparedStatement.setInt(5, Integer.parseInt(Info[4]));
                    preparedStatement.setInt(6, Integer.parseInt(Info[5]));
                    preparedStatement.setInt(7, Integer.parseInt(Info[6]));
                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            }
            case shipment -> {
                sql = "insert into shipment (shipment_id, item_name, item_price, item_type, from_city_id, to_city_id, shipping_id, log_time, total_time)" +
                        "values (?,?,?,?,?,?,?,?,?)";
                String[] Info = str.split(";");
                try {
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    //Insertion
                    preparedStatement.setInt(1, Integer.parseInt(Info[0]));
                    preparedStatement.setString(2, Info[1]);
                    preparedStatement.setInt(3, Integer.parseInt(Info[2]));
                    preparedStatement.setString(4, Info[3]);
                    preparedStatement.setInt(5, Integer.parseInt(Info[4]));
                    preparedStatement.setInt(6, Integer.parseInt(Info[5]));
                    preparedStatement.setInt(7, Integer.parseInt(Info[6]));
                    preparedStatement.setTimestamp(8, Timestamp.valueOf(Info[7]));
                    preparedStatement.setDate(9, Date.valueOf(Info[8]));
                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            }
        }
        return result;
    }














//    @Override
//    public String allContinentNames() {
//        getConnection();
//        StringBuilder sb = new StringBuilder();
//        String sql = "select continent from countries group by continent";
//        try {
//            Statement statement = con.createStatement();
//            resultSet = statement.executeQuery(sql);
//            while (resultSet.next()) {
//                sb.append(resultSet.getString("continent") + "\n");
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        } finally {
//            closeConnection();
//        }
//        return sb.toString();
//    }
//    @Override
//    public String continentsWithCountryCount() {
//        getConnection();
//        StringBuilder sb = new StringBuilder();
//        String sql = "select continent, count(*) countryNumber from countries group by continent;";
//        try {
//            Statement statement = con.createStatement();
//            resultSet = statement.executeQuery(sql);
//            while (resultSet.next()) {
//                sb.append(resultSet.getString("continent") + "\t");
//                sb.append(resultSet.getString("countryNumber"));
//                sb.append(System.lineSeparator());
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        } finally {
//            closeConnection();
//        }
//        return sb.toString();
//    }
//
//    @Override
//    public String FullInformationOfMoviesRuntime(int min, int max) {
//        getConnection();
//        StringBuilder sb = new StringBuilder();
//        String sql = "select m.title,c.country_name country,c.continent ,m.runtime " +
//                "from movies m " +
//                "join countries c on m.country=c.country_code " +
//                "where m.runtime between ? and ? order by runtime;";
//        try {
//            PreparedStatement preparedStatement = con.prepareStatement(sql);
//            preparedStatement.setInt(1, min);
//            preparedStatement.setInt(2, max);
//            resultSet = preparedStatement.executeQuery();
//            while (resultSet.next()) {
//                sb.append(resultSet.getString("runtime") + "\t");
//                sb.append(String.format("%-18s", resultSet.getString("country")));
//                sb.append(resultSet.getString("continent") + "\t");
//                sb.append(resultSet.getString("title") + "\t");
//                sb.append(System.lineSeparator());
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        } finally {
//            closeConnection();
//        }
//        return sb.toString();
//    }
//
//    @Override
//    public String findMovieById(int id) {
//        return null;
//    }

}
