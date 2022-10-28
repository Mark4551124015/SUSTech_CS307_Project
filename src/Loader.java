
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Calendar;
import java.util.Objects;

public class Loader  {
    private final String host = "localhost";
    private final String dbname = "sustc";
    private final String user = "postgres";
    private final String pwd = "314159";
    private final String port = "5432";
    private Connection con = null;
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
    public void insert (String filePath, int max) {
        getConnection();
        int cnt;
        int MAXRECORD = max;
        try {
            File csv = new File(filePath);
            csv.setReadable(true);
            csv.setWritable(true);
            InputStreamReader isr = null;
            BufferedReader br = null;
            try {
                isr = new InputStreamReader(new FileInputStream(csv), StandardCharsets.UTF_8);
                br = new BufferedReader(isr);
            } catch (Exception e) {
                e.printStackTrace();
            }
            String line = "";
            cnt = 0;
            line = br.readLine();
            Statement operation = con.createStatement();
            operation.executeUpdate("alter table ship disable trigger all;");
            operation.executeUpdate("alter table city disable trigger all;");
            operation.executeUpdate("alter table company disable trigger all;");
            operation.executeUpdate("alter table container disable trigger all;");
            operation.executeUpdate("alter table courier disable trigger all;");
            operation.executeUpdate("alter table delivery_retrieval disable trigger all;");
            operation.executeUpdate("alter table import_export_detail disable trigger all;");
            operation.executeUpdate("alter table portcity disable trigger all;");
            operation.executeUpdate("alter table shipment disable trigger all;");
            operation.executeUpdate("alter table shipping disable trigger all;");


            long startTime=System.currentTimeMillis();
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

            //tax cal after insertion
            String[] Info = line.split(",",-1);

            while ((line = br.readLine()) != null && cnt <= MAXRECORD) {
                ++cnt;
                Info = line.split(",",-1);
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




            }
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
            shipment.executeBatch();


            operation.executeUpdate("alter table ship enable trigger all");
            operation.executeUpdate("alter table city enable trigger all");
            operation.executeUpdate("alter table company enable trigger all");
            operation.executeUpdate("alter table container enable trigger all");
            operation.executeUpdate("alter table courier enable trigger all");
            operation.executeUpdate("alter table delivery_retrieval enable trigger all");
            operation.executeUpdate("alter table import_export_detail enable trigger all");
            operation.executeUpdate("alter table portcity enable trigger all");
            operation.executeUpdate("alter table shipment enable trigger all");
            operation.executeUpdate("alter table shipping enable trigger all");

            long endTime=System.currentTimeMillis();
            System.out.printf("Inserted: %d records, speed: %.2f records/s\n",MAXRECORD, (float)(MAXRECORD*1e3/(endTime-startTime)));
        } catch (Exception e) {
            System.err.println(e);
            closeConnection();
        }
    }
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

