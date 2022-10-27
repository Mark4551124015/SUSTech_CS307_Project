
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Calendar;
public class DatabaseManipulation{
    private Connection con = null;
    private ResultSet resultSet;
    private final String host = "localhost";
    private final String dbname = "sustc";
    private final String user = "postgres";
    private final String pwd = "314159";
    private final String port = "5432";
    public void getConnection() {
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
    public void closeConnection() {
        if (con != null) {
            try {
                con.close();
                con = null;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    //Add Records
    public long addOneRecord(Records type, String str) throws SQLException {
        long start = System.currentTimeMillis();
        String sql;
        String[] Info = str.split(",",-1);
        switch (type) {
            case city -> {
                    sql = "insert into city (name) values (?) on conflict do nothing";
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.executeUpdate();

            } // 1
            case portCity -> {
                    sql = "insert into portcity (name) values (?)  on conflict do nothing";
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.executeUpdate();

            } //1
            case company -> {
                    sql = "insert into company (name) values (?)  on conflict do nothing";
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.executeUpdate();

            } //1
            case container -> {
                    sql = "insert into container (code, type) values (?,?)  on conflict do nothing";
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.setString(2, Info[1]);
                    preparedStatement.executeUpdate();
            } //2
            case courier -> {
                    sql = "insert into courier (name, gender, birthday, phone_number, company, city)" +
                            "values (?,?,?,?,?,?) on conflict do nothing";
                    addOneRecord(Records.company, Info[4]);
                    addOneRecord(Records.city, Info[5]);
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.setString(2, Info[1]);
                    preparedStatement.setDate(3, Date.valueOf(Info[2]));
                    preparedStatement.setString(4, Info[3]);
                    preparedStatement.setString(5, Info[4]);
                    preparedStatement.setString(6, Info[5]);
                    preparedStatement.executeUpdate();

            } //6
            case ship -> {
                    sql = "insert into ship (name, company) values (?,?) on conflict do nothing";
                    addOneRecord(Records.company, Info[1]);
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.setString(2, Info[1]);
                    preparedStatement.executeUpdate();

            } //2
            case delivery_retrieval -> {
                    sql = "insert into delivery_retrieval (item_name,type,courier, date)" +
                            "values (?,?,?,?) on conflict do nothing";
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.setString(2, Info[1]);
                    preparedStatement.setString(3, Info[2]);
                    preparedStatement.setDate(4, Date.valueOf(Info[3]));
                    preparedStatement.executeUpdate();

            }
            case import_export_detail -> {
                    sql = "insert into import_export_detail (item_name,type, item_type, port_city, tax, date)" +
                            "values (?,?,?,?,?,?) on conflict do nothing";
                    addOneRecord(Records.portCity, Info[1]);
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.setString(2, Info[1]);
                    preparedStatement.setString(3, Info[2]);
                    preparedStatement.setString(4, Info[4]);
                    preparedStatement.setFloat(5, Float.parseFloat(Info[5]));
                    preparedStatement.setDate(6, Date.valueOf(Info[6]));
                    preparedStatement.executeUpdate();
            }
        }
        return System.currentTimeMillis() - start;
    }
    public long addFullRecords(String str) throws SQLException {
        long startTime=System.currentTimeMillis();
        String[] Info = str.split(",");
            Statement operation = con.createStatement();
//            operation.executeUpdate("alter table ship disable trigger all;");
//            operation.executeUpdate("alter table city disable trigger all;");
//            operation.executeUpdate("alter table company disable trigger all;");
//            operation.executeUpdate("alter table container disable trigger all;");
//            operation.executeUpdate("alter table courier disable trigger all;");
//            operation.executeUpdate("alter table delivery_retrieval disable trigger all;");
//            operation.executeUpdate("alter table import_export_detail disable trigger all;");
//            operation.executeUpdate("alter table portcity disable trigger all;");
//            operation.executeUpdate("alter table shipment disable trigger all;");
//            operation.executeUpdate("alter table shipping disable trigger all;");

            PreparedStatement company = con.prepareStatement("insert into company (name) values (?)  on conflict do nothing;");
            PreparedStatement exportCity = con.prepareStatement("insert into portcity (name) values (?)  on conflict do nothing;");
            PreparedStatement importCity = con.prepareStatement("insert into portcity (name) values (?)  on conflict do nothing;");

            PreparedStatement container = con.prepareStatement("insert into container (code, type) values (?,?)  on conflict do nothing;");
            PreparedStatement ship = con.prepareStatement("insert into ship (name, company) values (?,?)  on conflict do nothing;");
            PreparedStatement cityR = con.prepareStatement("insert into city (name) values (?)  on conflict do nothing;");
            PreparedStatement cityD = con.prepareStatement("insert into city (name) values (?)  on conflict do nothing;");

            PreparedStatement courierR = con.prepareStatement("insert into courier (name, gender, birthday, phone_number, company, city) values (?,?,?,?,?,?)  on conflict do nothing;");
            PreparedStatement courierD = con.prepareStatement("insert into courier (name, gender, birthday, phone_number, company, city) values (?,?,?,?,?,?)  on conflict do nothing;");

            PreparedStatement import_detail = con.prepareStatement("insert into import_export_detail (item_name,type, item_type, port_city, tax, date) values (?,?,?,?,?,?)  on conflict do nothing;");
            PreparedStatement export_detail = con.prepareStatement("insert into import_export_detail (item_name,type, item_type, port_city, tax, date) values (?,?,?,?,?,?)  on conflict do nothing;");
            PreparedStatement retrieval = con.prepareStatement("insert into delivery_retrieval (item_name,type,courier,date) values (?,?,?,?)  on conflict do nothing;");
            PreparedStatement delivery = con.prepareStatement("insert into delivery_retrieval (item_name,type,courier,date) values (?,?,?,?)  on conflict do nothing;");

            PreparedStatement shipping = con.prepareStatement("insert into shipping (item_name, ship, container) values (?,?,?)  on conflict do nothing;");
            PreparedStatement shipment = con.prepareStatement("insert into shipment (item_name, item_price, item_type, from_city, to_city, log_time) values (?,?,?,?,?,?)  on conflict do nothing;");

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

                shipment.setString(1, ItemName);
                shipment.setFloat(2,Float.parseFloat(ItemPrice));
                shipment.setString(3, ItemType);
                shipment.setString(4, RetrievalCity);
                shipment.setString(5, DeliveryCity);
                shipment.setTimestamp(6, Timestamp.valueOf(LogTime));
                shipment.addBatch();

                //single insertion
                company.setString(1,CompanyName);
                company.addBatch();

                cityR.setString(1,RetrievalCity);
                cityR.addBatch();

                courierR.setString(1,RetrievalCourier);
                courierR.setString(2,RetrievalCourierGender);
                courierR.setDate(3, CalBirth(RetrievalStartTime, Float.parseFloat(RetrievalCourierAge)));
                courierR.setString(4, RetrievalCourierPhoneNumber);
                courierR.setString(5, CompanyName);
                courierR.setString(6, RetrievalCity);
                courierR.addBatch();

                retrieval.setString(1,ItemName);
                retrieval.setString(2,"retrieval");
                retrieval.setString(3,RetrievalCourier);
                retrieval.setDate(4,Date.valueOf(RetrievalStartTime));
                retrieval.addBatch();

                if (!(ItemExportTime.isEmpty())) {
                    exportCity.setString(1, ItemExportCity);
                    exportCity.addBatch();

                    export_detail.setString(1, ItemName);
                    export_detail.setString(2, "export");
                    export_detail.setString(3, ItemType);
                    export_detail.setString(4, ItemExportCity);
                    export_detail.setFloat(5, Float.parseFloat(ItemExportTax));
                    export_detail.setDate(6, Date.valueOf(ItemExportTime));
                    export_detail.addBatch();
                }

                if (!(ShipName.isEmpty())) {
                    container.setString(1,ContainerCode);
                    container.setString(2,ContainerType);
                    container.addBatch();

                    ship.setString(1,ShipName);
                    ship.setString(2,CompanyName);
                    ship.addBatch();

                    shipping.setString(2,ShipName);
                    shipping.setString(3,ContainerCode);
                }else {
                    shipping.setString(2,null);
                    shipping.setString(3,null);
                }
                shipping.setString(1,ItemName);
                shipping.addBatch();


                if (!(ItemImportTime.isEmpty())) {
                    importCity.setString(1, ItemImportCity);
                    importCity.addBatch();

                    import_detail.setString(1, ItemName);
                    import_detail.setString(2, "import");
                    import_detail.setString(3, ItemType);
                    import_detail.setString(4, ItemImportCity);
                    import_detail.setFloat(5, Float.parseFloat(ItemImportTax));
                    import_detail.setDate(6, Date.valueOf(ItemImportTime));
                    import_detail.addBatch();
                }

                if (!(DeliveryFinishTime.isEmpty())) {
                    cityD.setString(1,DeliveryCity);
                    cityD.addBatch();

                    courierD.setString(1,DeliveryCourier);
                    courierD.setString(2,DeliveryCourierGender);
                    courierD.setDate(3, CalBirth(DeliveryFinishTime, Float.parseFloat(DeliveryCourierAge)));
                    courierD.setString(4, DeliveryCourierPhoneNumber);
                    courierD.setString(5, CompanyName);
                    courierD.setString(6, DeliveryCity);
                    courierD.addBatch();

                    delivery.setString(1,ItemName);
                    delivery.setString(2,"delivery");
                    delivery.setString(3,DeliveryCourier);
                    delivery.setDate(4,Date.valueOf(RetrievalStartTime));
                    delivery.addBatch();
                }
            shipment.executeBatch();
            company.executeBatch();
            exportCity.executeBatch();
            importCity.executeBatch();
            container.executeBatch();
            ship.executeBatch();
            cityR.executeBatch();
            cityD.executeBatch();
            courierR.executeBatch();
            courierD.executeBatch();
            import_detail.executeBatch();
            export_detail.executeBatch();
            retrieval.executeBatch();
            delivery.executeBatch();
            shipping.executeBatch();


//            operation.executeUpdate("alter table ship enable trigger all");
//            operation.executeUpdate("alter table city enable trigger all");
//            operation.executeUpdate("alter table company enable trigger all");
//            operation.executeUpdate("alter table container enable trigger all");
//            operation.executeUpdate("alter table courier enable trigger all");
//            operation.executeUpdate("alter table delivery_retrieval enable trigger all");
//            operation.executeUpdate("alter table import_export_detail enable trigger all");
//            operation.executeUpdate("alter table portcity enable trigger all");
//            operation.executeUpdate("alter table shipment enable trigger all");
//            operation.executeUpdate("alter table shipping enable trigger all");
            return System.currentTimeMillis() - startTime;
    }

    //Delete Records
    public long deleteByItemName(String name) throws SQLException {
        long startTime = System.currentTimeMillis();
        PreparedStatement shipment = con.prepareStatement("delete from shipment where item_name = ?");
        PreparedStatement shipping = con.prepareStatement("delete from shipping where item_name = ?");
        PreparedStatement rd_detail = con.prepareStatement("delete from delivery_retrieval where item_name = ?");
        PreparedStatement ie_detail = con.prepareStatement("delete from import_export_detail where item_name = ?");
        shipment.setString(1,name);
        shipping.setString(1,name);
        rd_detail.setString(1,name);
        ie_detail.setString(1,name);
        shipment.executeUpdate();
        shipping.executeUpdate();
        rd_detail.executeUpdate();
        ie_detail.executeUpdate();
        long endTime=System.currentTimeMillis();
        return endTime - startTime;
    }
    public void deleteByItemName(String[] name) throws SQLException {
        long startTime = System.currentTimeMillis();
        String tmp;
        PreparedStatement shipment = con.prepareStatement("delete from shipment where item_name = ?");
        PreparedStatement shipping = con.prepareStatement("delete from shipping where item_name = ?");
        PreparedStatement rd_detail = con.prepareStatement("delete from delivery_retrieval where item_name = ?");
        PreparedStatement ie_detail = con.prepareStatement("delete from import_export_detail where item_name = ?");
        for (int i = 0; i < name.length; i++) {
            tmp = name[i];
            shipment.setString(1,tmp);
            shipping.setString(1,tmp);
            rd_detail.setString(1,tmp);
            ie_detail.setString(1,tmp);
            shipment.addBatch();
            shipping.addBatch();
            rd_detail.addBatch();
            ie_detail.addBatch();
        }
        shipment.executeBatch();
        shipping.executeBatch();
        rd_detail.executeBatch();
        ie_detail.executeBatch();
        long endTime=System.currentTimeMillis();
        System.out.printf("Deleted: %d records, speed: %.2f records/s\n",name.length, (float)(name.length*1e3/(endTime-startTime)));
    }

    //Select Records
    public String selectTypes(String name) {
        getConnection();
        StringBuilder sb = new StringBuilder();
        String sql = "select continent from countries group by continent";
        try {
            Statement statement = con.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                sb.append(resultSet.getString("continent") + "\n");
            }
            sb.append(resultSet.first() + "\n");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
        return sb.toString();
    }
    public String selectByItemName(String name) {
        getConnection();
        StringBuilder sb = new StringBuilder();
        String sql = "select continent from countries group by continent";
        try {
            Statement statement = con.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                sb.append(resultSet.getString("continent") + "\n");
            }
            sb.append(resultSet.first() + "\n");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
        return sb.toString();
    }








    int getObjID(Records type, String arg) throws SQLException {
        String[] Info = arg.split(",",-1);
        switch(type){
            case city -> {
                    String sql ="select city_id from city where name = ?";
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        return resultSet.getInt("city_id");
                    }

            }
            case courier -> {
                    String sql ="select courier_id from courier where name = ?";
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        return resultSet.getInt("courier_id");
                    }

            }
            case portCity -> {
                    String sql ="select port_city_id from portcity where name = ?";
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        return resultSet.getInt("port_city_id");
                    }

            }
            case company -> {
                    String sql ="select company_id from company where name = ?";
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        return resultSet.getInt("company_id");
                    }

            }
            case container -> {
                    String sql ="select container_id from container where code = ?";
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        return resultSet.getInt("container_id");
                    }
            }
            case ship -> {
                    String sql ="select ship_id from ship where name = ?";
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        return resultSet.getInt("ship_id");
                    }

            }
            case delivery_retrieval -> {
                    String sql ="select dr_id from delivery_retrieval where item_name = ? and type = ?";
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.setString(2, Info[1]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        return resultSet.getInt("dr_id");
                    }

            }
            case import_export_detail -> {
                   String sql ="select port_id from import_export_detail where item_name = ? and type = ?";
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.setString(2,  Info[1]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        return resultSet.getInt("port_id");
                    }

            }
            case shipping -> {
                String sql = "select shipping_id from shipping where item_name = ?";
                PreparedStatement preparedStatement = con.prepareStatement(sql);
                preparedStatement.setString(1, Info[0]);
                resultSet = preparedStatement.executeQuery();
                if (resultSet.next()) {
                    return resultSet.getInt("shipping_id");
                }
            }
            case shipment -> {
                    String sql ="select shipment_id from shipment where item_name = ?";
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        return resultSet.getInt("shipment_id");
                    }
            }
        }
        return -1;
    }


    //Update Records

    //Select Records

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
    public static Date CalBirth (String str, float age) {
        Date date = Date.valueOf(str);
        Calendar birth = Calendar.getInstance();
        birth.setTime(date);
        birth.add(Calendar.YEAR, -1*(int)age);
        java.util.Date ret = birth.getTime();
        long temp = ret.getTime();
        Date out = new Date(temp);
        return out;
    }

}

//
