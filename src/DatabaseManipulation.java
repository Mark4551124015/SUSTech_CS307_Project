
import java.sql.*;
import java.util.Calendar;
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
        String sql, tmp;
        switch (type) {
            case city -> {
                try {
                    String[] Info = str.split(",");
                    //Check if exist already
                    if (getObjID(type, Info[0]) > 0) {
                        break;
                    }
                    //Insertion
                    sql = "insert into city (name) values (?)";
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    System.out.println(preparedStatement.toString());
                    preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            } // 1
            case portCity -> {
                try {
                    String[] Info = str.split(",");
                    //Check if exist already
                    if (getObjID(type, Info[0]) > 0) {
                        break;
                    }
                    //Insertion
                    sql = "insert into portcity (name) values (?)";
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    System.out.println(preparedStatement.toString());
                    preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            } //1
            case company -> {
                try {
                    String[] Info = str.split(",");
                    //Check if exist already
                    if (getObjID(type, Info[0]) > 0) {
                        break;
                    }
                    sql = "insert into company (name)" +
                            "values (?)";
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            } //1
            case itemType -> {
                try {
                    String[] Info = str.split(",");
                    //Check if exist already
                    if (getObjID(type, Info[0]) > 0) {
                        break;
                    }
                    sql = "insert into itemtype (item_type)" +
                            "values (?)";
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            }
            case container -> {
                try {
                    String[] Info = str.split(",");
                    //Check if exist already
                    if (getObjID(type, Info[0]) > 0) {
                        break;
                    }
                    sql = "insert into container (code, type) values (?,?)";


                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    //Insertion
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.setString(2, Info[1]);
                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            } //2
            case courier -> {
                try {
                    sql = "insert into courier (name, gender, birthday, phone_number, company_id, city_id)" +
                            "values (?,?,?,?,?,?)";
                    String[] Info = str.split(",");
                    if (getObjID(type, Info[0]) > 0) {
                        break;
                    }
                    if (getObjID(Records.company, Info[4]) < 0) {
                        addOneRecord(Records.company, Info[4]);
                    }
                    int company_id = getObjID(Records.company, Info[4]);

                    if (getObjID(Records.city, Info[5]) < 0) {
                        addOneRecord(Records.city, Info[5]);
                    }
                    int city_id = getObjID(Records.city, Info[5]);
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    //Insertion
                    //name gender birthday phoneNumber
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.setString(2, Info[1]);
                    preparedStatement.setDate(3, Date.valueOf(Info[2]));
                    preparedStatement.setInt(4, Integer.parseInt(Info[3]));
                    preparedStatement.setInt(5, company_id);
                    preparedStatement.setInt(6, city_id);
                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            } //6
            case ship -> {
                try {
                    sql = "insert into ship (name, company_id)" +
                            "values (?,?)";
                    String[] Info = str.split(",");
                    if (getObjID(type, Info[0]) > 0) {
                        break;
                    }
                    if (getObjID(Records.company, Info[1]) < 0) {
                        addOneRecord(Records.company, Info[1]);
                    }
                    int company_id = getObjID(Records.company, Info[1]);

                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    //Insertion
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.setInt(2, company_id);
                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            } //2

            //below is a record with more args
            case delivery_retrieval -> {
                //form courierName + type + date
                try {
                    sql = "insert into delivery_retrieval (courier_id, type, date)" +
                            "values (?,?,?)";
                    String[] Info = str.split(",");
                    int courier_id = getObjID(Records.courier,Info[0]);

                    if (getObjID(Records.delivery_retrieval, str) > 0) {
                        break;
                    }

                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    //Insertion
                    preparedStatement.setInt(1, courier_id);
                    preparedStatement.setString(2, Info[2]);
                    preparedStatement.setDate(3, Date.valueOf(Info[3]));

                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            }
            case import_export_detail -> {
                try {
                    sql = "insert into import_export_detail (type, port_city_id, tax, date)" +
                            "values (?,?,?,?)";
                    String[] Info = str.split(",");

                    if (getObjID(Records.portCity,Info[1]) < 0) {
                        addOneRecord(Records.portCity, Info[1]);
                    }
                    int port_city_id = getObjID(Records.portCity,Info[1]);
                    if (getObjID(Records.import_export_detail,str) > 0) {
                        break;
                    }

                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    //Insertion
                    preparedStatement.setString(1, Info[1]);
                    preparedStatement.setInt(2, port_city_id);
                    preparedStatement.setInt(3, Integer.parseInt(Info[3]));
                    preparedStatement.setDate(4, Date.valueOf(Info[4]));

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

    public int addFullRecords(String str) {
        getConnection();
        int result = 0;

        String[] Info = str.split(",");
        String tmp;
        //insert shipping
        try {
            //get Information
            String ItemName = Info[0];
            String ItemType = Info[1];
            String ItemPrice = Info[2];
            String RetrievalCity = Info[3];
            String RetrievalStartTime = Info[4];
            String RetrievalCourier = Info[5];
            String RetrievalCourierGender = Info[6];
            String RetrievalCourierPhoneNumber = Info[7];
            String RetrievalCourierAge = Info[8];
            String DeliveryFinishTime = Info[9];
            String DeliveryCity = Info[10];
            String DeliveryCourier = Info[11];
            String DeliveryCourierGender = Info[12];
            String DeliveryCourierPhoneNumber = Info[13];
            String DeliveryCourierAge = Info[14];
            String ItemExportCity = Info[15];
            String ItemExportTax = Info[16];
            String ItemExportTime = Info[17];
            String ItemImportCity = Info[18];
            String ItemImportTax = Info[19];
            String ItemImportTime = Info[20];
            String ContainerCode = Info[21];
            String ContainerType = Info[22];
            String ShipName = Info[23];
            String CompanyName = Info[24];
            String LogTime = Info[25];

            String sql = "insert into shipping (retrieval_id, export_id, ship_id, container_id, import_id, delivery_id)" +
                    "values (?,?,?,?,?,?)";

            //retrieval courier
            tmp = RetrievalCourier;
            if (getObjID(Records.courier, tmp) < 0) {
                Date birthday = CalBirth(RetrievalStartTime, Integer.parseInt(RetrievalCourierAge));
                tmp = RetrievalCourier + "," +
                        RetrievalCourierGender + "," +
                        birthday + "," +
                        Integer.parseInt(RetrievalCourierPhoneNumber) + "," +
                        CompanyName + "," +
                        RetrievalCity;
                addOneRecord(Records.courier, tmp);
            }

            //retrieval
            tmp = RetrievalCourier +
                    ",Retrieval," +
                    RetrievalStartTime;
            if (getObjID(Records.delivery_retrieval, tmp) < 0) {
                addOneRecord(Records.delivery_retrieval, tmp);
            }
            int retrieval_id = getObjID(Records.delivery_retrieval, tmp);

            //export
            tmp = "Export," +
                    ItemExportCity + "," +
                    ItemExportTax + "," +
                    ItemExportTime;
            if (getObjID(Records.import_export_detail, tmp) < 0) {
                addOneRecord(Records.import_export_detail, tmp);
            }
            int export_id = getObjID(Records.import_export_detail, tmp);

            //ship
            tmp = ShipName;
            if (getObjID(Records.ship, tmp) < 0) {
                tmp = ShipName + "," +
                        CompanyName;
                addOneRecord(Records.ship, tmp);
            }
            tmp = ShipName;
            int ship_id = getObjID(Records.ship, tmp);

            //container
            tmp = ContainerCode;
            if (getObjID(Records.container, tmp) < 0) {
                tmp = ContainerCode + "," +
                        ContainerType;
                addOneRecord(Records.container, tmp);
            }
            tmp = ContainerCode;
            int container_id = getObjID(Records.container, tmp);
            //import
            tmp = "Import," +
                    ItemImportCity + "," +
                    ItemImportTax + "," +
                    ItemImportTime;
            if (getObjID(Records.import_export_detail, tmp) < 0) {
                addOneRecord(Records.import_export_detail, tmp);
            }
            int import_id = getObjID(Records.import_export_detail, tmp);

            //delivery courier
            tmp = DeliveryCourier;
            if (getObjID(Records.courier, tmp) < 0) {
                Date birthday = CalBirth(DeliveryFinishTime, Integer.parseInt(DeliveryCourierAge));
                tmp = DeliveryCourier + "," +
                        DeliveryCourierGender + "," +
                        birthday + "," +
                        Integer.parseInt(DeliveryCourierPhoneNumber) + "," +
                        CompanyName + "," +
                        DeliveryCity;
                addOneRecord(Records.courier, tmp);
            }

            //delivery
            tmp = DeliveryCourier +
                    ",Delivery," +
                    DeliveryFinishTime;
            if (getObjID(Records.delivery_retrieval, tmp) < 0) {
                addOneRecord(Records.delivery_retrieval, tmp);
            }
            int delivery_id = getObjID(Records.delivery_retrieval, tmp);
            //Insertion
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setInt(1, retrieval_id);
            preparedStatement.setInt(2, export_id);
            preparedStatement.setInt(3, ship_id);
            preparedStatement.setInt(4, container_id);
            preparedStatement.setInt(5, import_id);
            preparedStatement.setInt(6, delivery_id);
            System.out.println(preparedStatement.toString());
            result = preparedStatement.executeUpdate();

            int shipping_id = getObjID(Records.shipping, str);

            //Item Type
            tmp = ItemType;
            if (getObjID(Records.itemType, tmp) < 0) {
                addOneRecord(Records.itemType, tmp);
            }
            int item_type_id = getObjID(Records.itemType, tmp);

            //update Tax






            sql = "insert into shipment (shipment_id, item_name, item_price, item_type_id, from_city_id, to_city_id,shipping_id,log_time)" +
                    "values (?,?,?,?,?,?,?,?)";
            PreparedStatement pS = con.prepareStatement(sql);

            //Insertion
            pS.setInt(1, );
            pS.setString(2, );
            pS.setInt(3, );
            pS.setString(4, );
            pS.setInt(5, );
            pS.setInt(6, );
            pS.setInt(7, );
            pS.setTimestamp(8,);
            pS.setDate(9, );
            System.out.println(pS.toString());
            result = pS.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
        return result;
    }


    int getObjID(Records type, String arg) {
        String[] Info = arg.split(";");
        switch(type){
            case city -> {
                try {
                    String sql ="select city_id from city where name = ?";
                    //Check if exist already
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        System.out.println("City: " + arg + " exist, id: " + resultSet.getInt("city_id"));
                        return resultSet.getInt("city_id");
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            }
            case portCity -> {
                try {
                    String sql ="select port_city_id from portcity where name = ?";
                    //Check if exist already
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        int id = resultSet.getInt("port_city_id");
                        System.out.println("Port City: " + arg + " exist, id: " + id);
                        return id;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            }
            case company -> {
                try {
                    String sql ="select company_id from company where name = ?";
                    //Check if exist already
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        int id =resultSet.getInt("company_id");
                        System.out.println("Company: " + arg + " exist, id: " + id);
                        return id;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            }
            case itemType -> {
                try {
                    String sql ="select item_type_id from itemtype where item_type = ?";
                    //Check if exist already
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        int id =resultSet.getInt("item_type_id");
                        System.out.println("Item Type: " + arg + " exist, id: " + id);
                        return id;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            }
            case container -> {
                try {
                    String sql ="select container_id from container where code = ?";
                    //Check if exist already
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        int id = resultSet.getInt("container_id");
                        System.out.println("Container: " + arg + " exist, id: " + id);
                        return id;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            }
            case ship -> {
                try {
                    String sql ="select ship_id from ship where name = ?";
                    //Check if exist already
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        int id = resultSet.getInt("ship_id");
                        System.out.println("Ship: " + arg + " exist, id: " + id);
                        return id;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            }
            case delivery_retrieval -> {
                try {
                    String sql ="select dr_id from delivery_retrieval where courier_id = ? and type = ? and date = ?";
                    int courier_id = getObjID(Records.courier, Info[0]);
                    //Check if exist already
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setInt(1, courier_id);
                    preparedStatement.setString(2, Info[1]);
                    preparedStatement.setDate(3, Date.valueOf(Info[2]));
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        int id = resultSet.getInt("dr_id");
                        System.out.println("Delivery/Retrieval: " + arg + " exist, id: " + id);
                        return id;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            }
            case import_export_detail -> {
                try {
                    String sql ="select port_id from import_export_detail where type = ? and port_city_id = ? and tax = ? and date = ?";
                    int port_city_id = getObjID(Records.portCity, Info[1]);
                    //Check if exist already
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.setInt(2, port_city_id);
                    preparedStatement.setInt(3, Integer.parseInt(Info[2]));
                    preparedStatement.setDate(4, Date.valueOf(Info[3]));
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        int id = resultSet.getInt("port_id");
                        System.out.println("Import/Export: " + arg + " exist, id: " + id);
                        return id;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }
            }
            case shipping -> {
                String tmp;
                //get Information
                String ItemName = Info[0];
                String ItemType = Info[1];
                String ItemPrice = Info[2];
                String RetrievalCity = Info[3];
                String RetrievalStartTime = Info[4];
                String RetrievalCourier = Info[5];
                String RetrievalCourierGender = Info[6];
                String RetrievalCourierPhoneNumber = Info[7];
                String RetrievalCourierAge = Info[8];
                String DeliveryFinishTime = Info[9];
                String DeliveryCity = Info[10];
                String DeliveryCourier = Info[11];
                String DeliveryCourierGender = Info[12];
                String DeliveryCourierPhoneNumber = Info[13];
                String DeliveryCourierAge = Info[14];
                String ItemExportCity = Info[15];
                String ItemExportTax = Info[16];
                String ItemExportTime = Info[17];
                String ItemImportCity = Info[18];
                String ItemImportTax = Info[19];
                String ItemImportTime = Info[20];
                String ContainerCode = Info[21];
                String ContainerType = Info[22];
                String ShipName = Info[23];
                String CompanyName = Info[24];
                String LogTime = Info[25];
                try {
                    String sql =
                            "select shipping_id from shipping where" +
                                    " retrieval_id = ? and" +
                                    " export_id = ? and" +
                                    " ship_id = ? and" +
                                    " container_id = ? and" +
                                    " import_id = ? and" +
                                    " delivery_id = ?";
                    //retrieval
                    tmp = RetrievalCourier +
                            ",Retrieval," +
                            RetrievalStartTime;
                    int retrieval_id = getObjID(Records.delivery_retrieval, tmp);
                    //export
                    tmp = "Export," +
                            ItemExportCity + "," +
                            ItemExportTax + "," +
                            ItemExportTime;
                    int export_id = getObjID(Records.import_export_detail, tmp);
                    //ship
                    tmp = ShipName;
                    int ship_id = getObjID(Records.ship, tmp);
                    //container
                    tmp = ContainerCode;
                    int container_id = getObjID(Records.container, tmp);
                    //import
                    tmp = "Import," +
                            ItemImportCity + "," +
                            ItemImportTax + "," +
                            ItemImportTime;
                    int import_id = getObjID(Records.import_export_detail, tmp);


                    //delivery
                    tmp = DeliveryCourier +
                            ",Delivery," +
                            DeliveryFinishTime;
                    int delivery_id = getObjID(Records.delivery_retrieval, tmp);


                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    //Insertion
                    preparedStatement.setInt(1, retrieval_id);
                    preparedStatement.setInt(2, export_id);
                    preparedStatement.setInt(3, ship_id);
                    preparedStatement.setInt(4, container_id);
                    preparedStatement.setInt(5, import_id);
                    preparedStatement.setInt(6, delivery_id);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        int id = resultSet.getInt("shipping_id");
                        System.out.println("Shipping: " + arg + " exist, id: " + id);
                        return id;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                } finally {
                    closeConnection();
                }

            }

        }
        return -1;
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
//            sb.append(resultSet.first() + "\n");
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
    public static Date CalBirth (String str, int age) {
        Date date = Date.valueOf(str);
        Calendar birth = Calendar.getInstance();
        birth.setTime(date);
        birth.add(Calendar.YEAR, -1*age);
        return (Date) birth.getTime();
    }

}

