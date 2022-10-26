
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.Calendar;
public class Loader  {
    private String host = "localhost";
    private String dbname = "sustc";
    private String user = "postgres";
    private String pwd = "314159";
    private String port = "5432";

    private ResultSet resultSet;
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
    public static int dr_index = 1;
    public static int ie_index = 1;
    public static int shipping_index = 1;
    public void insertFromFile (String filePath) {
        int cnt;
        int MAXRECORD = 10000;
        try {
            File csv = new File(filePath);
            csv.setReadable(true);
            csv.setWritable(true);
            InputStreamReader isr = null;
            BufferedReader br = null;
            try {
                isr = new InputStreamReader(new FileInputStream(csv), "UTF-8");
                br = new BufferedReader(isr);
            } catch (Exception e) {
                e.printStackTrace();
            }
            String line = "";
            DataManipulation dm = new DataFactory().createDataManipulation("database");
            cnt = 0;
            line = br.readLine();
            dm.getConnection();
            PreparedStatement preOperation = con.prepareStatement("alter table ? disable trigger all");
            preOperation.setString(1,"ship");
            preOperation.addBatch();
            preOperation.setString(1,"city");
            preOperation.addBatch();
            preOperation.setString(1,"company");
            preOperation.addBatch();
            preOperation.setString(1,"container");
            preOperation.addBatch();
            preOperation.setString(1,"courier");
            preOperation.addBatch();
            preOperation.setString(1,"delivery_retrieval");
            preOperation.addBatch();
            preOperation.setString(1,"import_export_detail");
            preOperation.addBatch();
            preOperation.setString(1,"itemtype");
            preOperation.addBatch();
            preOperation.setString(1,"portcity");
            preOperation.addBatch();
            preOperation.setString(1,"shipment");
            preOperation.addBatch();
            preOperation.setString(1,"shipping");
            preOperation.addBatch();
            preOperation.setString(1,"tax");
            preOperation.addBatch();
            preOperation.executeUpdate();

            long startTime=System.currentTimeMillis();
            PreparedStatement company = con.prepareStatement("insert into company (name) values (?)  on conflict do nothing");
            PreparedStatement exportCity = con.prepareStatement("insert into portcity (name) values (?)  on conflict do nothing");
            PreparedStatement importCity = con.prepareStatement("insert into portcity (name) values (?)  on conflict do nothing");

            PreparedStatement itemtype = con.prepareStatement("insert into itemtype (item_type) values (?)  on conflict do nothing");
            PreparedStatement container = con.prepareStatement("insert into container (code, type) values (?)  on conflict do nothing");
            PreparedStatement ship = con.prepareStatement("insert into ship (name, company) values (?)  on conflict do nothing");
            PreparedStatement cityR = con.prepareStatement("insert into city (name) values (?)  on conflict do nothing");
            PreparedStatement cityD = con.prepareStatement("insert into city (name) values (?)  on conflict do nothing");

            PreparedStatement courierR = con.prepareStatement("insert into courier (name, gender, birthday, phone_number, company, city) values (?)  on conflict do nothing");
            PreparedStatement courierD = con.prepareStatement("insert into courier (name, gender, birthday, phone_number, company, city) values (?)  on conflict do nothing");

            PreparedStatement import_detail = con.prepareStatement("insert into import_export_detail (type, port_city, tax, date) values (?)  on conflict do nothing");
            PreparedStatement export_detail = con.prepareStatement("insert into import_export_detail (type, port_city, tax, date) values (?)  on conflict do nothing");
            PreparedStatement retrieval = con.prepareStatement("insert into delivery_retrieval (courier, type, date) values (?)  on conflict do nothing");
            PreparedStatement delivery = con.prepareStatement("insert into delivery_retrieval (courier, type, date) values (?)  on conflict do nothing");

            PreparedStatement shipping = con.prepareStatement("insert into shipping (retrieval_id, export_id, ship, container, import_id, delivery_id) values (?)  on conflict do nothing");
            PreparedStatement shipment = con.prepareStatement("insert into shipment (item_name, item_price, item_type, from_city, to_city, shipping_id, log_time) values (?)  on conflict do nothing");
            PreparedStatement alterFK = con.prepareStatement("alter table all in tablespace disable trigger all");

            //tax cal after insertion

            while ((line = br.readLine()) != null && cnt <= MAXRECORD) {
                ++cnt;
                dm.addFullRecords(line);
                String[] Info = line.split(",",-1);
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
                //single insertion
                company.setString(1,CompanyName);
                company.addBatch();
                itemtype.setString(1,ItemType);
                itemtype.addBatch();
                cityR.setString(1,RetrievalCity);
                cityR.addBatch();

                courierR.setString(1,RetrievalCourier);
                courierR.setString(2,RetrievalCourierGender);
                courierR.setDate(3, CalBirth(RetrievalStartTime, Float.parseFloat(RetrievalCourierAge)));
                courierR.setString(4, RetrievalCourierPhoneNumber);
                courierR.setString(5, CompanyName);
                courierR.setString(6, RetrievalCity);
                courierR.addBatch();
                retrieval.setString(1,RetrievalCourier);
                retrieval.setString(2,"retrieval");
                retrieval.setDate(3,Date.valueOf(RetrievalStartTime));
                retrieval.addBatch();
                shipping.setInt(1,dr_index++);


                if (ItemExportCity!="") {
                    exportCity.setString(1, ItemExportCity);
                    export_detail.addBatch();
                    export_detail.setString(1, "export");
                    export_detail.setString(2, ItemExportCity);
                    export_detail.setFloat(3, Float.parseFloat(ItemExportTax));
                    export_detail.setDate(4, Date.valueOf(ItemExportTime));
                    export_detail.addBatch();
                    shipping.setInt(2,ie_index++);
                } else {
                    shipping.setNull(2,Types.INTEGER);
                }
                if (ShipName!="") {
                    container.setString(1,ContainerCode);
                    container.setString(2,ContainerType);
                    container.addBatch();
                    ship.setString(1,ShipName);
                    ship.setString(2,CompanyName);
                    ship.addBatch();
                    shipping.setString(3,ShipName);
                    shipping.setString(4,ContainerCode);
                }else {
                    shipping.setString(3,null);
                    shipping.setString(4,null);
                }
                if (ItemImportCity!="") {
                    importCity.setString(1, ItemImportCity);
                    importCity.addBatch();
                    import_detail.setString(1, "export");
                    import_detail.setString(2, ItemImportCity);
                    import_detail.setFloat(3, Float.parseFloat(ItemImportTax));
                    import_detail.setDate(4, Date.valueOf(ItemImportTime));
                    import_detail.addBatch();
                    shipping.setInt(5,ie_index++);
                }else {
                    shipping.setNull(5,Types.INTEGER);
                }
                if (DeliveryCourier!="") {
                    cityD.setString(1,DeliveryCity);
                    cityD.addBatch();
                    courierD.setString(1,DeliveryCourier);
                    courierD.setString(2,DeliveryCourierGender);
                    courierD.setDate(3, CalBirth(DeliveryFinishTime, Float.parseFloat(DeliveryCourierAge)));
                    courierD.setString(4, DeliveryCourierPhoneNumber);
                    courierD.setString(5, CompanyName);
                    courierD.setString(6, DeliveryCity);
                    courierD.addBatch();
                    delivery.setString(1,DeliveryCourier);
                    delivery.setString(2,"delivery");
                    delivery.setDate(3,Date.valueOf(DeliveryFinishTime));
                    delivery.addBatch();
                    shipping.setInt(6,dr_index++);
                }else {
                    shipping.setNull(6,Types.INTEGER);
                }
                shipping.addBatch();
                shipment.setString(1, ItemName);
                shipment.setFloat(2,Float.parseFloat(ItemPrice));
                shipment.setString(3, ItemType);
                shipment.setString(4, RetrievalCity);
                shipment.setString(5, DeliveryCity);
                shipment.setInt(6, shipping_index++);
                shipment.setTimestamp(7, Timestamp.valueOf(LogTime));
                shipment.addBatch();
            }

            preOperation = con.prepareStatement("alter table ? enable trigger all");
            preOperation.setString(1,"ship");
            preOperation.addBatch();
            preOperation.setString(1,"city");
            preOperation.addBatch();
            preOperation.setString(1,"company");
            preOperation.addBatch();
            preOperation.setString(1,"container");
            preOperation.addBatch();
            preOperation.setString(1,"courier");
            preOperation.addBatch();
            preOperation.setString(1,"delivery_retrieval");
            preOperation.addBatch();
            preOperation.setString(1,"import_export_detail");
            preOperation.addBatch();
            preOperation.setString(1,"itemtype");
            preOperation.addBatch();
            preOperation.setString(1,"portcity");
            preOperation.addBatch();
            preOperation.setString(1,"shipment");
            preOperation.addBatch();
            preOperation.setString(1,"shipping");
            preOperation.addBatch();
            preOperation.setString(1,"tax");
            preOperation.addBatch();
            preOperation.executeUpdate();




            long endTime=System.currentTimeMillis();
            System.out.println("Inserted data cnt: " + MAXRECORD +", costs: "+(endTime-startTime)/1000+"s");
        } catch (Exception e) {
            System.err.println(e);
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

