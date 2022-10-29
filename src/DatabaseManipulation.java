
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
        if (con == null) {
            getConnection();
        }
        long start = System.currentTimeMillis();
        String sql;
        String[] Info = str.split(",", -1);
        switch (type) {
            case city -> {
                sql = "insert IGNORE into city (name) values (?";
                PreparedStatement preparedStatement = con.prepareStatement(sql);
                preparedStatement.setString(1, Info[0]);
                preparedStatement.executeUpdate();

            } // 1
            case portCity -> {
                sql = "insert IGNORE into portcity (name) values (?)";
                PreparedStatement preparedStatement = con.prepareStatement(sql);
                preparedStatement.setString(1, Info[0]);
                preparedStatement.executeUpdate();

            } //1
            case company -> {
                sql = "insert IGNORE into company (name) values (?)";
                PreparedStatement preparedStatement = con.prepareStatement(sql);
                preparedStatement.setString(1, Info[0]);
                preparedStatement.executeUpdate();

            } //1
            case container -> {
                sql = "insert IGNORE into container (code, type) values (?,?)";
                PreparedStatement preparedStatement = con.prepareStatement(sql);
                preparedStatement.setString(1, Info[0]);
                preparedStatement.setString(2, Info[1]);
                preparedStatement.executeUpdate();
            } //2
            case courier -> {
                sql = "insert IGNORE into courier (name, gender, birthday, phone_number, company, city)" +
                        "values (?,?,?,?,?,?";
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
                sql = "insert IGNORE into ship (name, company) values (?,?";
                addOneRecord(Records.company, Info[1]);
                PreparedStatement preparedStatement = con.prepareStatement(sql);
                preparedStatement.setString(1, Info[0]);
                preparedStatement.setString(2, Info[1]);
                preparedStatement.executeUpdate();

            } //2
            case delivery_retrieval -> {
                sql = "insert IGNORE into delivery_retrieval (item_name,type,courier,city, date)" +
                        "values (?,?,?,?,?";
                PreparedStatement preparedStatement = con.prepareStatement(sql);
                preparedStatement.setString(1, Info[0]);
                preparedStatement.setString(2, Info[1]);
                preparedStatement.setString(3, Info[2]);
                preparedStatement.setString(4, Info[3]);

                preparedStatement.setDate(5, Date.valueOf(Info[4]));
                preparedStatement.executeUpdate();

            }
            case import_export_detail -> {
                sql = "insert IGNORE into import_export_detail (item_name,type, item_type, port_city, tax, date)" +
                        "values (?,?,?,?,?,?";
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
        if (con == null) {
            getConnection();
        }
        long startTime = System.currentTimeMillis();
        String[] Info = str.split(",", -1);
        PreparedStatement company = con.prepareStatement("insert IGNORE into company (name) values (?);");
        PreparedStatement exportCity = con.prepareStatement("insert IGNORE into portcity (name) values (?);");
        PreparedStatement importCity = con.prepareStatement("insert IGNORE into portcity (name) values (?);");

        PreparedStatement container = con.prepareStatement("insert IGNORE into container (code, type) values (?,?);");
        PreparedStatement ship = con.prepareStatement("insert IGNORE into ship (name, company) values (?,?);");
        PreparedStatement cityR = con.prepareStatement("insert IGNORE into city (name) values (?);");
        PreparedStatement cityD = con.prepareStatement("insert IGNORE into city (name) values (?);");

        PreparedStatement courierR = con.prepareStatement("insert IGNORE into courier (name, gender, birthday, phone_number, company, port_city) values (?,?,?,?,?,?);");
        PreparedStatement courierD = con.prepareStatement("insert IGNORE into courier (name, gender, birthday, phone_number, company, port_city) values (?,?,?,?,?,?);");

        PreparedStatement import_detail = con.prepareStatement("insert IGNORE into import_export_detail (item_name,type, item_type, port_city, tax, date) values (?,?,?,?,?,?);");
        PreparedStatement export_detail = con.prepareStatement("insert IGNORE into import_export_detail (item_name,type, item_type, port_city, tax, date) values (?,?,?,?,?,?);");
        PreparedStatement retrieval = con.prepareStatement("insert IGNORE into delivery_retrieval (item_name,type,courier,city,date) values (?,?,?,?,?);");
        PreparedStatement delivery = con.prepareStatement("insert IGNORE into delivery_retrieval (item_name,type,courier,city,date) values (?,?,?,?,?);");

        PreparedStatement shipping = con.prepareStatement("insert IGNORE into shipping (item_name, ship, container) values (?,?,?);");
        PreparedStatement shipment = con.prepareStatement("insert IGNORE into shipment (item_name, item_price, item_type, from_city, to_city, export_city, import_city, company, log_time) values (?,?,?,?,?,?,?,?,?);");

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
        shipment.setFloat(2, Float.parseFloat(ItemPrice));
        shipment.setString(3, ItemType);
        shipment.setString(4, RetrievalCity);
        shipment.setString(5, DeliveryCity);
        shipment.setString(6, ItemExportCity);
        shipment.setString(7, ItemImportCity);
        shipment.setString(8, CompanyName);
        shipment.setTimestamp(9, Timestamp.valueOf(LogTime));
        shipment.addBatch();
        //single insert IGNOREion


        exportCity.setString(1, ItemExportCity);
        exportCity.addBatch();
        importCity.setString(1, ItemImportCity);
        importCity.addBatch();

        courierR.setString(1, RetrievalCourier);
        courierR.setString(2, RetrievalCourierGender);
        courierR.setDate(3, CalBirth(RetrievalStartTime, Float.parseFloat(RetrievalCourierAge)));
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
            export_detail.setFloat(5, Float.parseFloat(ItemExportTax));
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
            import_detail.setFloat(5, Float.parseFloat(ItemImportTax));
            import_detail.setDate(6, Date.valueOf(ItemImportTime));
            import_detail.addBatch();
        }

        if (!(DeliveryFinishTime.isEmpty())) {

            courierD.setString(1, DeliveryCourier);
            courierD.setString(2, DeliveryCourierGender);
            courierD.setDate(3, CalBirth(DeliveryFinishTime, Float.parseFloat(DeliveryCourierAge)));
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
        if (con == null) {
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
        if (con == null) {
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
        System.out.printf("Deleted: %d records, speed: %.2f records/s\n", name.length, (float) (name.length * 1e3 / (endTime - startTime)));
    }

    public long deleteCity(String name) throws SQLException {
        long startTime = System.currentTimeMillis();
        if (con == null) {
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
        if (con == null) {
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
        System.out.printf("Deleted City: %d records, speed: %.2f records/s\n", name.length, (float) (name.length * 1e3 / (endTime - startTime)));
    }

    public long deleteCourier(String name) throws SQLException {
        if (con == null) {
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
        if (con == null) {
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
        System.out.printf("Deleted Courier: %d records, speed: %.2f records/s\n", name.length, (float) (name.length * 1e3 / (endTime - startTime)));
    }

    //Select Records
    public long selectShipmentByName(String Item) throws SQLException {
        long start = System.currentTimeMillis();
        if (con == null) {
            getConnection();
        }
        String sql = "select * from shipment where item_name = ?";
        String[] Info = new String[9];
        PreparedStatement preparedStatement = con.prepareStatement(sql);
        preparedStatement.setString(1, Item);
        resultSet = preparedStatement.executeQuery();
        System.out.println("SHIPMENT DETAILS: ");
        System.out.println("--------------------------------------------------------------------------------------------------------------------------------------------------------------------");
        System.out.printf("%15s %10s %15s %15s %15s %15s %15s %15s %20s\n", "Item Name", "Item Type", "Item Price", "From City", "To City", "Export City", "Import City", "Company", "Log Time");
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
            System.out.format("%15s %10s %15s %15s %15s %15s %15s %15s %20s\n", Info[0], Info[1], Info[2], Info[3], Info[4], Info[5], Info[6], Info[7], Info[8]);
        }
        return System.currentTimeMillis() - start;
    }

    public long selectShipmentByID(int id) throws SQLException {
        long start = System.currentTimeMillis();
        if (con == null) {
            getConnection();
        }
        String sql = "select * from shipment where shipment_id = ?";
        String[] Info = new String[9];
        PreparedStatement preparedStatement = con.prepareStatement(sql);
        preparedStatement.setInt(1, id);
        resultSet = preparedStatement.executeQuery();
        System.out.println("SHIPMENT DETAILS: ");
        System.out.println("--------------------------------------------------------------------------------------------------------------------------------------------------------------------");
        System.out.printf("%15s %10s %15s %15s %15s %15s %15s %15s %20s\n", "Item Name", "Item Type", "Item Price", "From City", "To City", "Export City", "Import City", "Company", "Log Time");
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
            System.out.format("%15s %10s %15s %15s %15s %15s %15s %15s %20s\n", Info[0], Info[1], Info[2], Info[3], Info[4], Info[5], Info[6], Info[7], Info[8]);
        }
        return System.currentTimeMillis() - start;
    }

    public long selectExportDetailByPortCity(String str) throws SQLException {
        long start = System.currentTimeMillis();
        if (con == null) {
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
        return System.currentTimeMillis() - start;
    }

    public long selectImportDetailByPortCity(String str) throws SQLException {
        long start = System.currentTimeMillis();
        if (con == null) {
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
        return System.currentTimeMillis() - start;
    }

    public long selectShippingInfoByItemName(String str) throws SQLException {
        long start = System.currentTimeMillis();
        if (con == null) {
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
        return System.currentTimeMillis() - start;
    }

    public long selectPortDetailByItem(String Item) throws SQLException {
        long start = System.currentTimeMillis();
        if (con == null) {
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
        return System.currentTimeMillis() - start;
    }

    public long selectDRDetailByItem(String Item) throws SQLException {
        long start = System.currentTimeMillis();
        if (con == null) {
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
        return System.currentTimeMillis() - start;
    }

    //Update Records
    public long updateByRecord(String str) throws SQLException {
        String[] Info = str.split(",", -1);
        if (getObjID(Records.courier, Info[0]) < 0) {
            System.out.println("Does not exist");
            return -1;
        }
        long startTime = System.currentTimeMillis();
        if (con == null) {
            getConnection();
        }
        PreparedStatement cityD = con.prepareStatement("insert IGNORE into city (name) values (?);");

        PreparedStatement container = con.prepareStatement("insert IGNORE into container (code, type) values (?,?);");
        PreparedStatement ship = con.prepareStatement("insert IGNORE into ship (name, company) values (?,?);");

        PreparedStatement courierD = con.prepareStatement("insert IGNORE into courier (name, gender, birthday, phone_number, company, port_city) values (?,?,?,?,?,?);");
        PreparedStatement import_detail = con.prepareStatement("insert IGNORE into import_export_detail (item_name,type, item_type, port_city, tax, date) values (?,?,?,?,?,?);");
        PreparedStatement export_detail = con.prepareStatement("insert IGNORE into import_export_detail (item_name,type, item_type, port_city, tax, date) values (?,?,?,?,?,?);");
        PreparedStatement delivery = con.prepareStatement("insert IGNORE into delivery_retrieval (item_name,type,courier,city,date) values (?,?,?,?,?);");

        PreparedStatement shipping = con.prepareStatement("insert IGNORE into shipping (item_name, ship, container) values (?,?,?);");
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


        if (!(ItemExportTime.isEmpty()) && getObjID(Records.import_export_detail, ItemName + ",export") < 0) {
            export_detail.setString(1, ItemName);
            export_detail.setString(2, "export");
            export_detail.setString(3, ItemType);
            export_detail.setString(4, ItemExportCity);
            export_detail.setFloat(5, Float.parseFloat(ItemExportTax));
            export_detail.setDate(6, Date.valueOf(ItemExportTime));
            export_detail.addBatch();
        }

        if (!(ShipName.isEmpty()) && getObjID(Records.import_export_detail, ItemName + ",export") < 0) {
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


        if (!(ItemImportTime.isEmpty()) && getObjID(Records.import_export_detail, ItemName + ",import") < 0) {
            import_detail.setString(1, ItemName);
            import_detail.setString(2, "import");
            import_detail.setString(3, ItemType);
            import_detail.setString(4, ItemImportCity);
            import_detail.setFloat(5, Float.parseFloat(ItemImportTax));
            import_detail.setDate(6, Date.valueOf(ItemImportTime));
            import_detail.addBatch();
        }

        if (!(DeliveryFinishTime.isEmpty()) && getObjID(Records.delivery_retrieval, ItemName + ",delivery") < 0) {

            courierD.setString(1, DeliveryCourier);
            courierD.setString(2, DeliveryCourierGender);
            courierD.setDate(3, CalBirth(DeliveryFinishTime, Float.parseFloat(DeliveryCourierAge)));
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
        String[] Info = str.split(",", -1);
        if (getObjID(Records.courier, Info[0]) < 0) {
            System.out.println("Does not exist");
            return -1;
        }
        long start = System.currentTimeMillis();
        if (con == null) {
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
    public void emptyTables() throws Exception{

        getConnection();

        String sql = "";

        String[] tables = new String[]{"city", "company", "courier", "container", "delivery_retrieval", "import_export_detail", "portcity", "ship", "shipment", "shipping"};
        for (String table : tables) {
            sql = "delete from " + table;
            Statement statement = con.createStatement();
            statement.executeUpdate(sql);
        }
//        System.out.println("All cleaned");
    }

    int getObjID(Records type, String arg) throws SQLException {
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

    public static Date CalBirth(String str, float age) {
        Date date = Date.valueOf(str);
        Calendar birth = Calendar.getInstance();
        birth.setTime(date);
        birth.add(Calendar.YEAR, -1 * (int) age);
        java.util.Date ret = birth.getTime();
        long temp = ret.getTime();
        Date out = new Date(temp);
        return out;
    }

}
//
