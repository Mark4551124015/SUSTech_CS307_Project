
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Calendar;
import java.util.Scanner;

public class DatabaseManipulation {
    private Connection con = null;
    private ResultSet resultSet;
    public void getConnection() {
        try {
            Class.forName("org.postgresql.Driver");

        } catch (Exception e) {
            System.err.println("Cannot find the PostgreSQL driver. Check CLASSPATH.");
            System.exit(1);
        }

        try {
            JDBC jb = new JDBC();
            con = jb.getNewCon();
        } catch (Exception e) {
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
        if (con==null) {
            getConnection();
        }
        long start = System.currentTimeMillis();
        String sql;
        String[] Info = str.split(",", -1);
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
                addOneRecord(Records.portCity, Info[5]);
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
                sql = "insert into delivery_retrieval (item_name,type,courier,city, date)" +
                        "values (?,?,?,?,?) on conflict do nothing";
                PreparedStatement preparedStatement = con.prepareStatement(sql);
                preparedStatement.setString(1, Info[0]);
                preparedStatement.setString(2, Info[1]);
                preparedStatement.setString(3, Info[2]);
                preparedStatement.setString(4, Info[3]);

                preparedStatement.setDate(5, Date.valueOf(Info[4]));
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
                preparedStatement.setDouble(5, Double.parseDouble(Info[5]));
                preparedStatement.setDate(6, Date.valueOf(Info[6]));
                preparedStatement.executeUpdate();
            }
        }
        return System.currentTimeMillis() - start;
    }
    public long addFullRecords(String str) throws SQLException {
        if (con==null) {
            getConnection();
        }
        long startTime = System.currentTimeMillis();
        String[] Info = str.split(",",-1);
        PreparedStatement company = con.prepareStatement("insert into company (name) values (?)  on conflict do nothing;");
        PreparedStatement exportCity = con.prepareStatement("insert into portcity (name) values (?)  on conflict do nothing;");
        PreparedStatement importCity = con.prepareStatement("insert into portcity (name) values (?)  on conflict do nothing;");

        PreparedStatement container = con.prepareStatement("insert into container (code, type) values (?,?)  on conflict do nothing;");
        PreparedStatement ship = con.prepareStatement("insert into ship (name, company) values (?,?)  on conflict do nothing;");
        PreparedStatement cityR = con.prepareStatement("insert into city (name) values (?)  on conflict do nothing;");
        PreparedStatement cityD = con.prepareStatement("insert into city (name) values (?)  on conflict do nothing;");

        PreparedStatement courierR = con.prepareStatement("insert into courier (name, gender, birthday, phone_number, company, port_city) values (?,?,?,?,?,?)  on conflict do nothing;");
        PreparedStatement courierD = con.prepareStatement("insert into courier (name, gender, birthday, phone_number, company, port_city) values (?,?,?,?,?,?)  on conflict do nothing;");

        PreparedStatement import_detail = con.prepareStatement("insert into import_export_detail (item_name,type, item_type, port_city, tax, date) values (?,?,?,?,?,?)  on conflict do nothing;");
        PreparedStatement export_detail = con.prepareStatement("insert into import_export_detail (item_name,type, item_type, port_city, tax, date) values (?,?,?,?,?,?)  on conflict do nothing;");
        PreparedStatement retrieval = con.prepareStatement("insert into delivery_retrieval (item_name,type,courier,city,date) values (?,?,?,?,?)  on conflict do nothing;");
        PreparedStatement delivery = con.prepareStatement("insert into delivery_retrieval (item_name,type,courier,city,date) values (?,?,?,?,?)  on conflict do nothing;");

        PreparedStatement shipping = con.prepareStatement("insert into shipping (item_name, ship, container) values (?,?,?)  on conflict do nothing;");
        PreparedStatement shipment = con.prepareStatement("insert into shipment (item_name, item_price, item_type, from_city, to_city, export_city, import_city, company, log_time) values (?,?,?,?,?,?,?,?,?)  on conflict do nothing;");

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

        company.setString(1, CompanyName);
        company.addBatch();
        cityD.setString(1, DeliveryCity);
        cityD.addBatch();
        cityR.setString(1, RetrievalCity);
        cityR.addBatch();

        shipment.setString(1, ItemName);
        shipment.setDouble(2, Double.parseDouble(ItemPrice));
        shipment.setString(3, ItemType);
        shipment.setString(4, RetrievalCity);
        shipment.setString(5, DeliveryCity);
        shipment.setString(6,ItemExportCity);
        shipment.setString(7,ItemImportCity);
        shipment.setString(8,CompanyName);
        shipment.setTimestamp(9, Timestamp.valueOf(LogTime));
        shipment.addBatch();
        //single insertion


        exportCity.setString(1, ItemExportCity);
        exportCity.addBatch();
        importCity.setString(1, ItemImportCity);
        importCity.addBatch();

        courierR.setString(1, RetrievalCourier);
        courierR.setString(2, RetrievalCourierGender);
        courierR.setDate(3, CalBirth(RetrievalStartTime, Double.parseDouble(RetrievalCourierAge)));
        courierR.setString(4, RetrievalCourierPhoneNumber);
        courierR.setString(5, CompanyName);
        courierR.setString(6, ItemExportCity);
        courierR.addBatch();

        retrieval.setString(1, ItemName);
        retrieval.setString(2, "retrieval");
        retrieval.setString(3, RetrievalCourier);
        retrieval.setString(4, RetrievalCity);
        retrieval.setDate(5, Date.valueOf(RetrievalStartTime));
        retrieval.addBatch();

        if (!(ItemExportTime.isEmpty())) {
            export_detail.setString(1, ItemName);
            export_detail.setString(2, "export");
            export_detail.setString(3, ItemType);
            export_detail.setString(4, ItemExportCity);
            export_detail.setDouble(5, Double.parseDouble(ItemExportTax));
            export_detail.setDate(6, Date.valueOf(ItemExportTime));
            export_detail.addBatch();
        }

        if (!(ShipName.isEmpty())) {
            container.setString(1, ContainerCode);
            container.setString(2, ContainerType);
            container.addBatch();

            ship.setString(1, ShipName);
            ship.setString(2, CompanyName);
            ship.addBatch();

            shipping.setString(2, ShipName);
            shipping.setString(3, ContainerCode);
        } else {
            shipping.setString(2, null);
            shipping.setString(3, null);
        }
        shipping.setString(1, ItemName);
        shipping.addBatch();


        if (!(ItemImportTime.isEmpty())) {
            import_detail.setString(1, ItemName);
            import_detail.setString(2, "import");
            import_detail.setString(3, ItemType);
            import_detail.setString(4, ItemImportCity);
            import_detail.setDouble(5, Double.parseDouble(ItemImportTax));
            import_detail.setDate(6, Date.valueOf(ItemImportTime));
            import_detail.addBatch();
        }

        if (!(DeliveryFinishTime.isEmpty())) {

            courierD.setString(1, DeliveryCourier);
            courierD.setString(2, DeliveryCourierGender);
            courierD.setDate(3, CalBirth(DeliveryFinishTime, Double.parseDouble(DeliveryCourierAge)));
            courierD.setString(4, DeliveryCourierPhoneNumber);
            courierD.setString(5, CompanyName);
            courierD.setString(6, ItemImportCity);
            courierD.addBatch();

            delivery.setString(1, ItemName);
            delivery.setString(2, "delivery");
            delivery.setString(3, DeliveryCourier);
            delivery.setString(4, DeliveryCity);
            delivery.setDate(5, Date.valueOf(DeliveryFinishTime));
            delivery.addBatch();
        }
        company.executeBatch();
        exportCity.executeBatch();
        importCity.executeBatch();
        cityR.executeBatch();
        cityD.executeBatch();

        shipment.executeBatch();
        container.executeBatch();
        ship.executeBatch();
        courierR.executeBatch();
        courierD.executeBatch();
        import_detail.executeBatch();
        export_detail.executeBatch();
        retrieval.executeBatch();
        delivery.executeBatch();
        shipping.executeBatch();

        return System.currentTimeMillis() - startTime;
    }
    //Delete Records
    public long deleteByItemName(String name) throws SQLException {
        if (con==null) {
            getConnection();
        }
        long startTime = System.currentTimeMillis();
        PreparedStatement shipment = con.prepareStatement("delete from shipment where item_name = ?");
        PreparedStatement shipping = con.prepareStatement("delete from shipping where item_name = ?");
        PreparedStatement rd_detail = con.prepareStatement("delete from delivery_retrieval where item_name = ?");
        PreparedStatement ie_detail = con.prepareStatement("delete from import_export_detail where item_name = ?");
        shipment.setString(1, name);
        shipping.setString(1, name);
        rd_detail.setString(1, name);
        ie_detail.setString(1, name);
        shipping.executeUpdate();
        rd_detail.executeUpdate();
        ie_detail.executeUpdate();
        shipment.executeUpdate();
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }
    public void deleteByItemName(String[] name) throws SQLException {
        if (con==null) {
            getConnection();
        }
        long startTime = System.currentTimeMillis();
        String tmp;
        PreparedStatement shipment = con.prepareStatement("delete from shipment where item_name = ?");
        PreparedStatement shipping = con.prepareStatement("delete from shipping where item_name = ?");
        PreparedStatement rd_detail = con.prepareStatement("delete from delivery_retrieval where item_name = ?");
        PreparedStatement ie_detail = con.prepareStatement("delete from import_export_detail where item_name = ?");
        for (int i = 0; i < name.length; i++) {
            tmp = name[i];
            shipment.setString(1, tmp);
            shipping.setString(1, tmp);
            rd_detail.setString(1, tmp);
            ie_detail.setString(1, tmp);
            shipment.addBatch();
            shipping.addBatch();
            rd_detail.addBatch();
            ie_detail.addBatch();
        }

        shipping.executeBatch();
        rd_detail.executeBatch();
        ie_detail.executeBatch();
        shipment.executeBatch();
        long endTime = System.currentTimeMillis();
        System.out.printf("Deleted: %d records, speed: %.2f records/s\n", name.length, (Double) (name.length * 1e3 / (endTime - startTime)));
    }
    public long deleteCity(String name) throws SQLException {
        long startTime = System.currentTimeMillis();
        if (con==null) {
            getConnection();
        }
        PreparedStatement city = con.prepareStatement("delete from city where name = ?");

        city.setString(1, name);
        city.executeUpdate();
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }
    public void deleteCity(String[] name) throws SQLException {
        long startTime = System.currentTimeMillis();
        if (con==null) {
            getConnection();
        }
        String tmp;
        PreparedStatement city = con.prepareStatement("delete from city where name = ?");
        for (int i = 0; i < name.length; i++) {
            tmp = name[i];
            city.setString(1, tmp);
            city.addBatch();
        }
        city.executeBatch();
        long endTime = System.currentTimeMillis();
        System.out.printf("Deleted City: %d records, speed: %.2f records/s\n", name.length, (Double) (name.length * 1e3 / (endTime - startTime)));
    }
    public long deleteCourier(String name) throws SQLException {
        if (con==null) {
            getConnection();
        }
        long startTime = System.currentTimeMillis();
        PreparedStatement courier = con.prepareStatement("delete from courier where name = ?");
        courier.setString(1, name);
        courier.executeUpdate();
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }
    public void deleteCourier(String[] name) throws SQLException {

        long startTime = System.currentTimeMillis();
        if (con==null) {
            getConnection();
        }
        String tmp;
        PreparedStatement courier = con.prepareStatement("delete from courier where name = ?");
        for (int i = 0; i < name.length; i++) {
            tmp = name[i];
            courier.setString(1, tmp);
            courier.addBatch();
        }
        courier.executeBatch();
        long endTime = System.currentTimeMillis();
        System.out.printf("Deleted Courier: %d records, speed: %.2f records/s\n", name.length, (Double) (name.length * 1e3 / (endTime - startTime)));
    }
    //Select Records
    public long selectShipmentByName(String Item) throws SQLException {
        long start = System.currentTimeMillis();
        if (con==null) {
            getConnection();
        }
        String sql = "select * from shipment where item_name = ?";
        String[] Info = new String[9];
        PreparedStatement preparedStatement = con.prepareStatement(sql);
        preparedStatement.setString(1, Item);
        resultSet = preparedStatement.executeQuery();
        System.out.println("SHIPMENT DETAILS: ");
        System.out.println("--------------------------------------------------------------------------------------------------------------------------------------------------------------------");
        System.out.printf("%15s %10s %15s %15s %15s %15s %15s %15s %20s\n",  "Item Name", "Item Type", "Item Price","From City", "To City","Export City", "Import City", "Company", "Log Time");
        System.out.println("--------------------------------------------------------------------------------------------------------------------------------------------------------------------");
        if (resultSet.next()) {
            Info[0] = resultSet.getString("item_name");
            Info[1] = resultSet.getString("item_type");
            Info[2] = "¥" + resultSet.getInt("item_price");
            Info[3] = resultSet.getString("from_city");
            Info[4] = resultSet.getString("to_city");
            Info[5] = resultSet.getString("export_city");
            Info[6] = resultSet.getString("import_city");
            Info[7] = resultSet.getString("company");
            Info[8] = resultSet.getString("log_time");
            System.out.format("%15s %10s %15s %15s %15s %15s %15s %15s %20s\n", Info[0], Info[1], Info[2], Info[3], Info[4], Info[5], Info[6],Info[7],Info[8]);
        }
        return System.currentTimeMillis()-start;
    }
    public long selectShipmentByID(int id) throws SQLException {
        long start = System.currentTimeMillis();
        if (con==null) {
            getConnection();
        }
        String sql = "select * from shipment where shipment_id = ?";
        String[] Info = new String[9];
        PreparedStatement preparedStatement = con.prepareStatement(sql);
        preparedStatement.setInt(1, id);
        resultSet = preparedStatement.executeQuery();
        System.out.println("SHIPMENT DETAILS: ");
        System.out.println("--------------------------------------------------------------------------------------------------------------------------------------------------------------------");
        System.out.printf("%15s %10s %15s %15s %15s %15s %15s %15s %20s\n",  "Item Name", "Item Type", "Item Price","From City", "To City","Export City", "Import City", "Company", "Log Time");
        System.out.println("--------------------------------------------------------------------------------------------------------------------------------------------------------------------");
        if (resultSet.next()) {
            Info[0] = resultSet.getString("item_name");
            Info[1] = resultSet.getString("item_type");
            Info[2] = "¥" + resultSet.getInt("item_price");
            Info[3] = resultSet.getString("from_city");
            Info[4] = resultSet.getString("to_city");
            Info[5] = resultSet.getString("export_city");
            Info[6] = resultSet.getString("import_city");
            Info[7] = resultSet.getString("company");
            Info[8] = resultSet.getString("log_time");
            System.out.format("%15s %10s %15s %15s %15s %15s %15s %15s %20s\n", Info[0], Info[1], Info[2], Info[3], Info[4], Info[5], Info[6],Info[7],Info[8]);
        }
        return System.currentTimeMillis()-start;
    }
    public long selectExportDetailByPortCity(String str) throws SQLException {
        long start = System.currentTimeMillis();
        if (con==null) {
            getConnection();
        }
        String sql = "select * from import_export_detail where port_city = ? and type = 'export'";
        String[] Info = new String[5];
        PreparedStatement preparedStatement = con.prepareStatement(sql);
        preparedStatement.setString(1, str);
        resultSet = preparedStatement.executeQuery();

        System.out.println("EXPORT DETAILS: ");
        System.out.println("----------------------------------------------------------");
        System.out.printf("%10s %20s %10s %10s\n", "City", "Item Name", "Tax", "Date");
        System.out.println("----------------------------------------------------------");
        while (resultSet.next()) {
            Info[0] = resultSet.getString("port_city");
            Info[1] = resultSet.getString("item_name");
            Info[2] = "¥" + resultSet.getInt("tax");
            Info[3] = resultSet.getString("date");
            System.out.format("%10s %20s %10s %10s\n", Info[0], Info[1], Info[2], Info[3]);
        }
        return System.currentTimeMillis()-start;
    }
    public long selectImportDetailByPortCity(String str) throws SQLException {
        long start = System.currentTimeMillis();
        if (con==null) {
            getConnection();
        }
        String sql = "select * from import_export_detail where port_city = ? and type = 'import'";
        String[] Info = new String[5];
        PreparedStatement preparedStatement = con.prepareStatement(sql);
        preparedStatement.setString(1, str);
        resultSet = preparedStatement.executeQuery();

        System.out.println("IMPORT DETAILS: ");
        System.out.println("----------------------------------------------------------");
        System.out.printf("%10s %20s %10s %10s\n", "City", "Item Name", "Tax", "Date");
        System.out.println("----------------------------------------------------------");
        while (resultSet.next()) {
            Info[0] = resultSet.getString("port_city");
            Info[1] = resultSet.getString("item_name");
            Info[2] = "¥" + resultSet.getInt("tax");
            Info[3] = resultSet.getString("date");
            System.out.format("%10s %20s %10s %10s\n", Info[0], Info[1], Info[2], Info[3]);
        }
        return System.currentTimeMillis()-start;
    }
    public long selectShippingInfoByItemName(String str) throws SQLException {
        long start = System.currentTimeMillis();
        if (con==null) {
            getConnection();
        }
        String sql = "select * from shipping where item_name = ?";
        String[] Info = new String[3];
        PreparedStatement preparedStatement = con.prepareStatement(sql);
        preparedStatement.setString(1, str);
        resultSet = preparedStatement.executeQuery();

        System.out.println("SHIPPING DETAILS: ");
        System.out.println("----------------------------------------------------------");
        System.out.printf("%15s %17s %15s\n", "Item Name", "Ship", "Container");
        System.out.println("----------------------------------------------------------");
        while (resultSet.next()) {
            Info[0] = resultSet.getString("item_name");
            Info[1] = resultSet.getString("ship");
            Info[2] = resultSet.getString("container");
            System.out.format("%15s %15s %15s\n", Info[0], Info[1], Info[2]);
        }
        return System.currentTimeMillis()-start;
    }
    public long selectPortDetailByItem(String Item) throws SQLException {
        long start = System.currentTimeMillis();
        if (con==null) {
            getConnection();
        }
        String sql = "select * from import_export_detail where item_name = ?";
        String[] Info = new String[5];
        PreparedStatement preparedStatement = con.prepareStatement(sql);
        preparedStatement.setString(1, Item);
        resultSet = preparedStatement.executeQuery();
//        resultSet.next();
        System.out.println("IMPORT/EXPORT DETAILS: ");
        System.out.println("----------------------------------------------------------------------------");
        System.out.printf("%20s %10s %15s %15s %10s\n", "Item Name", "City", "Tax", "Date", "Type");
        System.out.println("----------------------------------------------------------------------------");
        while (resultSet.next()) {
            Info[0] = resultSet.getString("item_name");
            Info[1] = resultSet.getString("port_city");
            Info[2] = "¥" + resultSet.getInt("tax");
            Info[3] = resultSet.getString("date");
            Info[4] = resultSet.getString("type");
            System.out.format("%20s %10s %15s %15s %10s\n", Info[0], Info[1], Info[2], Info[3], Info[4]);
        }
        return System.currentTimeMillis()-start;
    }
    public long selectDRDetailByItem(String Item) throws SQLException {
        long start = System.currentTimeMillis();
        if (con==null) {
            getConnection();
        }
        String sql = "select * from delivery_retrieval where item_name = ?";
        String[] Info = new String[4];
        PreparedStatement preparedStatement = con.prepareStatement(sql);
        preparedStatement.setString(1, Item);
        resultSet = preparedStatement.executeQuery();
//        resultSet.next();
        System.out.println("RETRIEVAL/DELIVERY DETAILS: ");
        System.out.println("-----------------------------------------------------------------------------");
        System.out.printf("%20s %10s %15s %15s %10s\n", "Item Name", "Courier", "Date", "City", "Type");
        System.out.println("-----------------------------------------------------------------------------");
        while (resultSet.next()) {
            Info[0] = resultSet.getString("item_name");
            Info[1] = resultSet.getString("courier");
            Info[2] = resultSet.getString("city");

            Info[3] = resultSet.getString("date");

            Info[4] = resultSet.getString("type");
            System.out.format("%20s %10s %15s %10s\n", Info[0], Info[1], Info[2], Info[3]);
        }
        return System.currentTimeMillis()-start;
    }
    //Update Records
    public long updateByRecord(String str) throws SQLException {
        String[] Info = str.split(",",-1);
        if (getObjID(Records.courier,Info[0]) < 0) {
            System.out.println("Does not exist");
            return -1;
        }
        long startTime = System.currentTimeMillis();
        if (con==null) {
            getConnection();
        }
        PreparedStatement cityD = con.prepareStatement("insert into city (name) values (?)  on conflict do nothing;");

        PreparedStatement container = con.prepareStatement("insert into container (code, type) values (?,?)  on conflict do nothing;");
        PreparedStatement ship = con.prepareStatement("insert into ship (name, company) values (?,?)  on conflict do nothing;");

        PreparedStatement courierD = con.prepareStatement("insert into courier (name, gender, birthday, phone_number, company, port_city) values (?,?,?,?,?,?)  on conflict do nothing;");
        PreparedStatement import_detail = con.prepareStatement("insert into import_export_detail (item_name,type, item_type, port_city, tax, date) values (?,?,?,?,?,?)  on conflict do nothing;");
        PreparedStatement export_detail = con.prepareStatement("insert into import_export_detail (item_name,type, item_type, port_city, tax, date) values (?,?,?,?,?,?)  on conflict do nothing;");
        PreparedStatement delivery = con.prepareStatement("insert into delivery_retrieval (item_name,type,courier,city,date) values (?,?,?,?,?)  on conflict do nothing;");

        PreparedStatement shipping = con.prepareStatement("insert into shipping (item_name, ship, container) values (?,?,?)  on conflict do nothing;");
        PreparedStatement shipment = con.prepareStatement("update shipment set log_time = ? where item_name = ?");

        String ItemName = Info[0];
        String ItemType = Info[1];

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

        shipment.setString(2, ItemName);
        shipment.setTimestamp(1, Timestamp.valueOf(LogTime));
        shipment.addBatch();


        if (!(ItemExportTime.isEmpty()) && getObjID(Records.import_export_detail, ItemName+",export")<0) {
            export_detail.setString(1, ItemName);
            export_detail.setString(2, "export");
            export_detail.setString(3, ItemType);
            export_detail.setString(4, ItemExportCity);
            export_detail.setDouble(5, Double.parseDouble(ItemExportTax));
            export_detail.setDate(6, Date.valueOf(ItemExportTime));
            export_detail.addBatch();
        }

        if (!(ShipName.isEmpty())&& getObjID(Records.import_export_detail, ItemName+",export")<0) {
            container.setString(1, ContainerCode);
            container.setString(2, ContainerType);
            container.addBatch();

            ship.setString(1, ShipName);
            ship.setString(2, CompanyName);
            ship.addBatch();

            shipping.setString(2, ShipName);
            shipping.setString(3, ContainerCode);
            shipping.setString(1, ItemName);
            shipping.addBatch();
        }



        if (!(ItemImportTime.isEmpty())&& getObjID(Records.import_export_detail, ItemName+",import")<0) {
            import_detail.setString(1, ItemName);
            import_detail.setString(2, "import");
            import_detail.setString(3, ItemType);
            import_detail.setString(4, ItemImportCity);
            import_detail.setDouble(5, Double.parseDouble(ItemImportTax));
            import_detail.setDate(6, Date.valueOf(ItemImportTime));
            import_detail.addBatch();
        }

        if (!(DeliveryFinishTime.isEmpty())&& getObjID(Records.delivery_retrieval, ItemName+",delivery")<0) {

            courierD.setString(1, DeliveryCourier);
            courierD.setString(2, DeliveryCourierGender);
            courierD.setDate(3, CalBirth(DeliveryFinishTime, Double.parseDouble(DeliveryCourierAge)));
            courierD.setString(4, DeliveryCourierPhoneNumber);
            courierD.setString(5, CompanyName);
            courierD.setString(6, ItemImportCity);
            courierD.addBatch();

            delivery.setString(1, ItemName);
            delivery.setString(2, "delivery");
            delivery.setString(3, DeliveryCourier);
            delivery.setString(4, DeliveryCity);
            delivery.setDate(5, Date.valueOf(DeliveryFinishTime));
            delivery.addBatch();
        }
        cityD.executeBatch();
        shipment.executeBatch();
        container.executeBatch();
        ship.executeBatch();
        courierD.executeBatch();
        import_detail.executeBatch();
        export_detail.executeBatch();
        delivery.executeBatch();
        shipping.executeBatch();

        return System.currentTimeMillis() - startTime;

    }
    public long updateCourier(String str) throws SQLException {
        String[] Info = str.split(",",-1);
        if (getObjID(Records.courier,Info[0]) < 0) {
            System.out.println("Does not exist");
            return -1;
        }
        long start = System.currentTimeMillis();
        if (con==null) {
            getConnection();
        }
        String sql = "update courier set gender = ?, birthday=?, phone_number=?, company=?, port_city=? where name = ?";

        addOneRecord(Records.company, Info[4]);
        addOneRecord(Records.city, Info[5]);
        PreparedStatement preparedStatement = con.prepareStatement(sql);
        preparedStatement.setString(1, Info[1]);
        preparedStatement.setDate(2, Date.valueOf(Info[2]));
        preparedStatement.setString(3, Info[3]);
        preparedStatement.setString(4, Info[4]);
        preparedStatement.setString(5, Info[5]);
        preparedStatement.setString(6, Info[0]);
        preparedStatement.executeUpdate();
        return System.currentTimeMillis() - start;
    }
    //Query
    public void QueryServedContainer(int expired) throws Exception{
        if (con==null) {
            getConnection();
        }
        Scanner sc = new Scanner(System.in);
        String Info;
        String sql = "select distinct type from container";
        PreparedStatement pS = con.prepareStatement(sql);
        resultSet = pS.executeQuery();
        System.out.println("CONTAINER TYPES: ");
        System.out.println("----------------------------------------------------------------------------------------------------");
        while (resultSet.next()) {
            Info= resultSet.getString("type");
            System.out.printf(Info+"   ");
        }
        System.out.println();
        System.out.println("----------------------------------------------------------------------------------------------------");
        System.out.println("Please enter the type of container:");
        String type = sc.nextLine();
        long startTime= System.currentTimeMillis();
        sql = "with tmp1 as" +
                "    (select c.code, date as importDate, ied.item_name" +
                "    from shipping" +
                "        join import_export_detail ied on shipping.item_name = ied.item_name" +
                "        join container c on shipping.container = c.code" +
                "    where c.type = ? and ied.type = 'import')," +
                "    tmp2 as" +
                "    (select c.code, date as exportDate, ied.item_name" +
                "    from shipping" +
                "        join import_export_detail ied on shipping.item_name = ied.item_name" +
                "        join container c on shipping.container = c.code" +
                "         where c.type = ? and ied.type = 'export')" +
                " select DISTINCT  tmp1.code, sum(importDate - exportDate) as served" +
                " from tmp2  join tmp1 on tmp1.item_name = tmp2.item_name" +
                " group by tmp1.code" +
                " having sum(importDate - exportDate) >= ?" +
                " order by served desc";
        pS = con.prepareStatement(sql);
        pS.setString((int)1,type);
        pS.setString((int)2,type);
        pS.setInt((int)3,expired);
        resultSet = pS.executeQuery();
        long endTime = System.currentTimeMillis();
        System.out.println("CONTAINER INFO: ");
        System.out.println("-----------------------------------------------------------------------------");
        System.out.printf("%20s %10s\n", "Code", "Served");
        System.out.println("-----------------------------------------------------------------------------");
        int len = 0;
        while (resultSet.next()) {
            len++;
            String code = resultSet.getString("code");
            int served = resultSet.getInt("served");
            System.out.format("%20s %10d\n", code, served);
        }
        System.out.println("-----------------------------------------------------------------------------");
        System.out.println("Queried "+ len+" containers, taking "+ (endTime- startTime)+"ms");
    }
    public void QueryBestCourier(String city, String inputCompany, int range) throws Exception{
        long startTime= System.currentTimeMillis();
        if (con==null) {
            getConnection();
        }
        String sql = "select courier, count(*), company" +
                "    from delivery_retrieval join courier c on c.name = delivery_retrieval.courier\n" +
                "    where city = ? and company = ?" +
                "    group by courier,company" +
                "    order by count(*) desc";
        PreparedStatement pS = con.prepareStatement(sql);
        pS.setString((int)1,city);
        pS.setString((int)2,inputCompany);
        resultSet = pS.executeQuery();
        long endTime = System.currentTimeMillis();
        System.out.println("COURIER INFO: ");
        System.out.println("------------------------------------------------------------");
        System.out.printf("%10s %10s %15s %15s\n", "Courier", "Count", "Company", "City");
        System.out.println("------------------------------------------------------------");
        int len = 0;
        while (resultSet.next() && len <= range) {
            len++;
            String courier = resultSet.getString("courier");
            String company = resultSet.getString("company");
            int count = resultSet.getInt("count");
            System.out.format("%10s %10d %15s %15s\n", courier, count, company, city);
        }
        System.out.println("------------------------------------------------------------");
        System.out.println("Queried "+ len+" results, taking "+(endTime-startTime)+"ms");
    }
    public void QueryBestPort(String Item_type, String Type) throws Exception{
        long startTime= System.currentTimeMillis();
        if (con==null) {
            getConnection();
        }

        String sql = "with newest as (" +
                "    select max(date) as newdate, port_city, item_type" +
                "    from import_export_detail as ied" +
                "    group by port_city, item_type" +
                "    having item_type = ?" +
                "    )," +
                "    temp as (select d.port_city, tax/item_price as taxRate, d.item_type, d.date from newest," +
                "                       shipment join import_export_detail d on shipment.item_name = d.item_name" +
                "                        where shipment.item_type = newest.item_type" +
                "                        and date = newdate" +
                "                        and type = ?" +
                "                        and newest.port_city = import_city" +
                "                        order by taxRate)" +
                "select * from temp where taxRate = (select  min(taxRate) from temp);";

        PreparedStatement pS = con.prepareStatement(sql);
        pS.setString((int)1,Item_type);
        pS.setString((int)2,Type);
        resultSet = pS.executeQuery();
        long endTime = System.currentTimeMillis();
        resultSet.next();
        System.out.println("-----------------------------------------------------------------------");
            String Port = resultSet.getString("port_city");
            Double taxrate = resultSet.getDouble("taxrate");

            System.out.printf("The best port for (%s) to (%s) is (%s), tax rate is (%f)\n", Item_type, Type, Port, taxrate);
        System.out.println("-----------------------------------------------------------------------");
        System.out.println("Queried "+ 1+" results, taking "+(endTime-startTime)+"ms");
    }
    public void emptyTables() throws Exception{

        getConnection();

        String sql ="";
        Statement Statement = con.createStatement();
        con.setAutoCommit(false);
        sql = "truncate shipment,import_export_detail,delivery_retrieval,city,company,courier,portcity,ship,shipping,container";
        Statement.executeUpdate(sql);
        con.commit();
        con.setAutoCommit(true);
        System.out.println("All cleaned");
    }
    int getObjID (Records type, String arg) throws SQLException {
    String[] Info = arg.split(",", -1);
    switch (type) {
        case city -> {
            String sql = "select city_id from city where name = ?";
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setString(1, Info[0]);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt("city_id");
            }

        }
        case courier -> {
            String sql = "select courier_id from courier where name = ?";
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setString(1, Info[0]);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt("courier_id");
            }

        }
        case portCity -> {
            String sql = "select port_city_id from portcity where name = ?";
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setString(1, Info[0]);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt("port_city_id");
            }

        }
        case company -> {
            String sql = "select company_id from company where name = ?";
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setString(1, Info[0]);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt("company_id");
            }

        }
        case container -> {
            String sql = "select container_id from container where code = ?";
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setString(1, Info[0]);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt("container_id");
            }
        }
        case ship -> {
            String sql = "select ship_id from ship where name = ?";
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setString(1, Info[0]);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt("ship_id");
            }

        }
        case delivery_retrieval -> {
            String sql = "select dr_id from delivery_retrieval where item_name = ? and type = ?";
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setString(1, Info[0]);
            preparedStatement.setString(2, Info[1]);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt("dr_id");
            }

        }
        case import_export_detail -> {
            String sql = "select port_id from import_export_detail where item_name = ? and type = ?";
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setString(1, Info[0]);
            preparedStatement.setString(2, Info[1]);
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
            String sql = "select shipment_id from shipment where item_name = ?";
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
    public static Date CalBirth (String str, Double age) {
        Date date = Date.valueOf(str);
        Calendar birth = Calendar.getInstance();
        birth.setTime(date);
        birth.add(Calendar.YEAR, (int)(-1*age));
        java.util.Date ret = birth.getTime();
        long temp = ret.getTime();
        Date out = new Date(temp);
        return out;
    }

}
//
