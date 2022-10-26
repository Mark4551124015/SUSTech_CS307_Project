
import java.sql.*;
import java.util.Calendar;
public class DatabaseManipulation implements DataManipulation {
    private Connection con = null;
    private ResultSet resultSet;
    private String host = "localhost";
    private String dbname = "sustc";
    private String user = "postgres";
    private String pwd = "314159";
    private String port = "5432";
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

    @Override
    public int addOneRecord(Records type, String str) throws SQLException {
        int result = 0;
        String sql, tmp;
        switch (type) {
            case city -> {

                    String[] Info = str.split(",",-1);
                    //Check if exist already
//                    if (getObjID(type, Info[0]) > 0) {
//                        break;
//                    }
                    //Insertion
                    sql = "insert into city (name) values (?) on conflict do nothing";
//                    getConnection();

                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
//                    System.out.println(preparedStatement.toString());
                    preparedStatement.executeUpdate();

            } // 1
            case portCity -> {

                    String[] Info = str.split(",",-1);
                    //Check if exist already
//                    if (getObjID(type, Info[0]) > 0) {
//                        break;
//                    }
                    //Insertion
                    sql = "insert into portcity (name) values (?)  on conflict do nothing";
//                    getConnection();
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
//                    System.out.println(preparedStatement.toString());
                    preparedStatement.executeUpdate();

            } //1
            case company -> {

                    String[] Info = str.split(",",-1);
                    //Check if exist already
//                    if (getObjID(type, Info[0]) > 0) {
//                        break;
//                    }
                    sql = "insert into company (name) " +
                            "values (?)  on conflict do nothing";
//                    getConnection();
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
//                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();

            } //1
            case itemType -> {

                    String[] Info = str.split(",");
                    //Check if exist already
//                    if (getObjID(type, Info[0]) > 0) {
//                        break;
//                    }
                    sql = "insert into itemtype (item_type) " +
                            "values (?) on  conflict do nothing";
//                    getConnection();
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
//                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();

            }
            case container -> {

                    String[] Info = str.split(",");
                    //Check if exist already
//                    if (getObjID(type, Info[0]) > 0) {
//                        break;
//                    }
                    sql = "insert into container (code, type) values (?,?)  on conflict do nothing";

//                    getConnection();
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    //Insertion
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.setString(2, Info[1]);
//                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();

            } //2
            case courier -> {

                    sql = "insert into courier (name, gender, birthday, phone_number, company, city)" +
                            "values (?,?,?,?,?,?) on conflict do nothing";
                    String[] Info = str.split(",");

//                    if (getObjID(Records.company, Info[4]) < 0) {
                        addOneRecord(Records.company, Info[4]);
//                    }

//                    if (getObjID(Records.city, Info[5]) < 0) {
                        addOneRecord(Records.city, Info[5]);
//                    }
//                    if (getObjID(type, Info[0]) > 0) {
//                        break;
//                    }
//                    getConnection();
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    //Insertion
                    //name gender birthday phoneNumber
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.setString(2, Info[1]);
                    preparedStatement.setDate(3, Date.valueOf(Info[2]));
                    preparedStatement.setString(4, Info[3]);
                    preparedStatement.setString(5, Info[4]);
                    preparedStatement.setString(6, Info[5]);
//                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();

            } //6
            case ship -> {

                    sql = "insert into ship (name, company)" +
                            "values (?,?) on conflict do nothing";
                    String[] Info = str.split(",");
//                    if (getObjID(type, Info[0]) > 0) {
//                        break;
//                    }
//                    if (getObjID(Records.company, Info[1]) < 0) {
                        addOneRecord(Records.company, Info[1]);
//                    }
//                    getConnection();

                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    //Insertion
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.setString(2, Info[1]);
//                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();

            } //2
            case tax -> {

                    String[] Info = str.split(",",-1);

                    //Check if exist already
                    if (taxDetailExist(Info[0],Info[1])) {
                        break;
                    }
                    //Insertion
                    sql = "insert into tax (port_city, item_type, last_update) values (?,?,?) on conflict do nothing";
//                    getConnection();
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.setString(2, Info[1]);
                    preparedStatement.setDate(3, Date.valueOf(Info[2]));
//                    System.out.println(preparedStatement.toString());
                    preparedStatement.executeUpdate();

            }

            //below is a record with more args
            case delivery_retrieval -> {
                //form courierName + type + date

                    sql = "insert into delivery_retrieval (courier, type, date)" +
                            "values (?,?,?) on conflict do nothing";
                    String[] Info = str.split(",",-1);

//                    if (getObjID(Records.delivery_retrieval, str) > 0) {
//                        break;
//                    }
//                    getConnection();
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    //Insertion
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.setString(2, Info[1]);
                    preparedStatement.setDate(3, Date.valueOf(Info[2]));

//                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();

            }
            case import_export_detail -> {

                    sql = "insert into import_export_detail (type, port_city, tax, date)" +
                            "values (?,?,?,?) on conflict do nothing";
                    String[] Info = str.split(",",-1);

//                    if (getObjID(Records.portCity,Info[1]) < 0) {
                        addOneRecord(Records.portCity, Info[1]);
//                    }
//                    if (getObjID(Records.import_export_detail,str) > 0) {
//                        break;
//                    }
//                    getConnection();
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    //Insertion
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.setString(2, Info[1]);
                    preparedStatement.setFloat(3, Float.parseFloat(Info[2]));
                    preparedStatement.setDate(4, Date.valueOf(Info[3]));

//                    System.out.println(preparedStatement.toString());
                    result = preparedStatement.executeUpdate();

            }
        }
        return result;
    }
    public int addFullRecords(String str) throws SQLException {


//        getConnection();
        int result = 0;
        String[] Info = str.split(",",-1);
        if (getObjID(Records.shipment, Info[0]) > 0) {
            return -1;
        }
        String tmp;
        //insert shipping
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

            String sql = "insert into shipping (retrieval_id, export_id, ship, container, import_id, delivery_id)" +
                    " values (?,?,?,?,?,?) on conflict do nothing";
            //retrieval stuff
            int retrieval_id = -1;
            int export_id = -1;
            int ship_id = -1;
            int container_id= -1;
            int import_id = -1;
            int delivery_id = -1;
            if (RetrievalStartTime!="") {
                //retrieval courier
                tmp = RetrievalCourier;
//                if (getObjID(Records.courier, tmp) < 0) {
                    Date birthday = CalBirth(RetrievalStartTime, Integer.parseInt(RetrievalCourierAge));
                    tmp = RetrievalCourier + "," +
                            RetrievalCourierGender + "," +
                            birthday.toString() + "," +
                            RetrievalCourierPhoneNumber + "," +
                            CompanyName + "," +
                            RetrievalCity;
                    addOneRecord(Records.courier, tmp);
//                }

                //retrieval
                tmp = RetrievalCourier +
                        ",Retrieval," +
                        RetrievalStartTime;
//                if (getObjID(Records.delivery_retrieval, tmp) < 0) {
                    addOneRecord(Records.delivery_retrieval, tmp);
//                }
                retrieval_id = getObjID(Records.delivery_retrieval, tmp);
            }
            //export
            if (ItemExportTime!="") {
                tmp = "Export," +
                        ItemExportCity + "," +
                        ItemExportTax + "," +
                        ItemExportTime;
//                if (getObjID(Records.import_export_detail, tmp) < 0) {
                    addOneRecord(Records.import_export_detail, tmp);
//                }
                export_id = getObjID(Records.import_export_detail, tmp);
            }
            //ship
            if (ShipName != "") {
                tmp = ShipName;
//                if (getObjID(Records.ship, tmp) < 0) {
                    tmp = ShipName + "," +
                            CompanyName;
                    addOneRecord(Records.ship, tmp);
//                }
            }

            //container
            if (ContainerCode!="") {
                tmp = ContainerCode;
//                if (getObjID(Records.container, tmp) < 0) {
                    tmp = ContainerCode + "," +
                            ContainerType;
                    addOneRecord(Records.container, tmp);
//                }
            }

            //import
            if (ItemImportTime!="") {
                tmp = "Import," +
                        ItemImportCity + "," +
                        ItemImportTax + "," +
                        ItemImportTime;
//                if (getObjID(Records.import_export_detail, tmp) < 0) {
                    addOneRecord(Records.import_export_detail, tmp);
//                }
                import_id = getObjID(Records.import_export_detail, tmp);
            }

            if (DeliveryCourier!="") {
                //delivery courier
                tmp = DeliveryCourier;
//                if (getObjID(Records.courier, tmp) < 0) {

                    Date birthday = CalBirth(DeliveryFinishTime, Float.parseFloat(DeliveryCourierAge));

                    tmp = DeliveryCourier + "," +
                            DeliveryCourierGender + "," +
                            birthday + "," +
                            DeliveryCourierPhoneNumber + "," +
                            CompanyName + "," +
                            DeliveryCity;
                    addOneRecord(Records.courier, tmp);
//                }

                //delivery
                tmp = DeliveryCourier +
                        ",Delivery," +
                        DeliveryFinishTime;
//                if (getObjID(Records.delivery_retrieval, tmp) < 0) {
                    addOneRecord(Records.delivery_retrieval, tmp);
//                }
                delivery_id = getObjID(Records.delivery_retrieval, tmp);
            }


            //Insertion
//            getConnection();
            PreparedStatement preparedStatement = con.prepareStatement(sql);

            if (retrieval_id>0) {
                preparedStatement.setInt(1, retrieval_id);
            } else {
                preparedStatement.setNull(1,Types.INTEGER);
            }
            if (export_id>0) {
                preparedStatement.setInt(2, export_id);
            } else {
                preparedStatement.setNull(2,Types.INTEGER);
            }
            if (ShipName!="") {
                preparedStatement.setString(3, ShipName);
            } else {
                preparedStatement.setString(3,null);
            }
            if (ContainerCode!="") {
                preparedStatement.setString(4, ContainerCode);
            } else {
                preparedStatement.setString(4,null);
            }
            if (import_id>0) {
                preparedStatement.setInt(5, import_id);
            } else {
                preparedStatement.setNull(5,Types.INTEGER);
            }
            if (delivery_id>0) {
                preparedStatement.setInt(6, delivery_id);
            } else {
                preparedStatement.setNull(6,Types.INTEGER);
            }

//            System.out.println(preparedStatement.toString());

            result = preparedStatement.executeUpdate();

            int shipping_id = getObjID(Records.shipping, str);
            //Item Type
            tmp = ItemType;
//            if (getObjID(Records.itemType, tmp) < 0) {
                addOneRecord(Records.itemType, tmp);
//            }

            //update Tax



            //time

            if (ItemExportTime!="") {
                //export detail
                double exportTaxRate = Float.parseFloat(ItemPrice) / Float.parseFloat(ItemExportTax);
                Date exportTime = Date.valueOf(ItemExportTime);
                if (!taxDetailExist(ItemExportCity, ItemType)) {
                    tmp = ItemExportCity + "," + ItemType + "," + "1000-01-01";
                    addOneRecord(Records.tax, tmp);
                }
                if (exportTime.after(getLastUpdate(Records.tax, ItemExportCity+","+ItemType))) {
                    //update TaxRate
                }
                // TODO: 2022/10/26 update Tax Rate


            }
            //import detail
            if (ItemImportTime!="") {
                double importTaxRate = Float.parseFloat(ItemPrice)/Float.parseFloat(ItemImportTax);
                Date importTime = Date.valueOf(ItemImportTime);
                if (!taxDetailExist(ItemImportCity, ItemType)) {
                    tmp = ItemImportCity +","+ItemType+","+"1000-01-01";
                    addOneRecord(Records.tax,tmp);
                }
                if (importTime.after(getLastUpdate(Records.tax, ItemImportCity+","+ItemType))) {
                    //update TaxRate
                }

            }



            sql = "insert into shipment (item_name, item_price, item_type, from_city, to_city,shipping_id,log_time)" +
                    "values (?,?,?,?,?,?,?) on conflict do nothing";
//            getConnection();
            PreparedStatement pS = con.prepareStatement(sql);

            Timestamp log_time = Timestamp.valueOf(LogTime);
            //Insertion
            pS.setString(1, ItemName);
            pS.setInt(2, Integer.parseInt(ItemPrice));
            pS.setString(3, ItemType);
            pS.setString(4, RetrievalCity);
            pS.setString(5, DeliveryCity);
            pS.setInt(6, shipping_id);
            pS.setTimestamp((int)7, log_time);
//            System.out.println(pS.toString());
            result = pS.executeUpdate();
        } catch (SQLException e) {
           System.err.println(e);
        } finally {
//            closeConnection();
        }
        return result;
    }
    int getObjID(Records type, String arg) throws SQLException {
        String[] Info = arg.split(",",-1);
        switch(type){
            case city -> {
                    String sql ="select city_id from city where name = ?";
                    //Check if exist already
//                    getConnection();
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        return resultSet.getInt("city_id");
                    }

            }
            case courier -> {
                    String sql ="select courier_id from courier where name = ?";
                    //Check if exist already
//                    getConnection();
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        return resultSet.getInt("courier_id");
                    }

            }
            case portCity -> {

                    String sql ="select port_city_id from portcity where name = ?";
                    //Check if exist already
//                    getConnection();
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        int id = resultSet.getInt("port_city_id");
                        return id;
                    }

            }
            case company -> {
                    String sql ="select company_id from company where name = ?";
                    //Check if exist already
//                    getConnection();

                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        int id =resultSet.getInt("company_id");
                        return id;
                    }

            }
            case itemType -> {

                    String sql ="select item_type_id from itemtype where item_type = ?";
                    //Check if exist already
//                    getConnection();

                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        int id =resultSet.getInt("item_type_id");
                        return id;
                    }

            }
            case container -> {

                    String sql ="select container_id from container where code = ?";
                    //Check if exist already
//                    getConnection();

                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        int id = resultSet.getInt("container_id");
                        return id;
                    }

            }
            case ship -> {

                    String sql ="select ship_id from ship where name = ?";
                    //Check if exist already
//                    getConnection();
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        int id = resultSet.getInt("ship_id");
                        return id;
                    }

            }
            case delivery_retrieval -> {

                    String sql ="select dr_id from delivery_retrieval where courier = ? and type = ? and date = ?";
                    //Check if exist already
//                    getConnection();
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.setString(2, Info[1]);
                    preparedStatement.setDate(3, Date.valueOf(Info[2]));
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        int id = resultSet.getInt("dr_id");
                        return id;
                    }

            }
            case import_export_detail -> {
                   String sql ="select port_id from import_export_detail where type = ? and port_city = ? and tax = ? and date = ?";
                    //Check if exist already
//                    getConnection();
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.setString(2,  Info[1]);
                    preparedStatement.setFloat(3, Float.parseFloat(Info[2]));
                    preparedStatement.setDate(4, Date.valueOf(Info[3]));
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        int id = resultSet.getInt("port_id");
                        return id;
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

                    String sql =
                            "select shipping_id from shipping where retrieval_id = ?";

                    //retrieval stuff
                    int retrieval_id = -1;
                    if (RetrievalStartTime!="") {
                        //retrieval courier
                        tmp = RetrievalCourier;
//                        if (getObjID(Records.courier, tmp) < 0) {
                            Date birthday = CalBirth(RetrievalStartTime, Integer.parseInt(RetrievalCourierAge));
                            tmp = RetrievalCourier + "," +
                                    RetrievalCourierGender + "," +
                                    birthday.toString() + "," +
                                    RetrievalCourierPhoneNumber + "," +
                                    CompanyName + "," +
                                    RetrievalCity;
                            addOneRecord(Records.courier, tmp);
//                        }

                        //retrieval
                        tmp = RetrievalCourier +
                                ",Retrieval," +
                                RetrievalStartTime;
//                        if (getObjID(Records.delivery_retrieval, tmp) < 0) {
                            addOneRecord(Records.delivery_retrieval, tmp);
//                        }
                        retrieval_id = getObjID(Records.delivery_retrieval, tmp);
                    }



                    //Insertion
//                    getConnection();
                    PreparedStatement preparedStatement = con.prepareStatement(sql);

                    if (retrieval_id>0) {
                        preparedStatement.setInt(1, retrieval_id);
                    }
                    resultSet = preparedStatement.executeQuery();

                    if (resultSet.next()) {
                        int id = resultSet.getInt("shipping_id");
                        return id;
                    }



            }
            case shipment -> {

                    String sql ="select shipment_id from shipment where item_name = ?";
                    //Check if exist already
//                    getConnection();
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
    Date getLastUpdate(Records type, String arg) throws SQLException {
        String[] Info = arg.split(",",-1);
        switch (type) {
            case tax -> {

                    String sql ="select last_update from tax where port_city = ? and item_type = ?";
                    //Check if exist already
//                    getConnection();
                    PreparedStatement preparedStatement = con.prepareStatement(sql);
                    preparedStatement.setString(1, Info[0]);
                    preparedStatement.setString(2, Info[1]);
                    resultSet = preparedStatement.executeQuery();
                    if (resultSet.next()) {
                        return resultSet.getDate("last_update");
                    }
            }
        }
        return null;
    }
    boolean taxDetailExist(String port_city, String item_type) throws SQLException {

            String sql ="select * from tax where port_city = ?" +
                    "and item_type = ?";
            //Check if exist already
//                getConnection();
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setString(1, port_city);
            preparedStatement.setString(2, item_type);
            resultSet = preparedStatement.executeQuery();
            return resultSet.next();

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

