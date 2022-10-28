
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Calendar;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Loader  {

    public void loadFromFile (String filePath, int max) {
        System.out.println("Start loading...");
        int cnt;
        int MAXRECORD = max;
        try {
            JDBC jb = new JDBC();
            Connection con = jb.getNewCon();
            File csv = new File(filePath);
            csv.setReadable(true);
            csv.setWritable(true);
            InputStreamReader isr = null;
            BufferedReader br = null;
            isr = new InputStreamReader(new FileInputStream(csv), StandardCharsets.UTF_8);
            br = new BufferedReader(isr);
            String line = "";
            cnt = 0;
            line = br.readLine();

            Statement operation = con.createStatement();
            con.setAutoCommit(false);
            long startTime=System.currentTimeMillis();
            

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
            con.commit();

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

            //tax cal after insertion
            String[] Info = new String[50];
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
            String LogTime ;

            while ((line = br.readLine()) != null && cnt <= MAXRECORD) {
                ++cnt;
                Info = line.split(",",-1);
                 ItemName = Info[0];
                 ItemType = Info[1];
                 ItemPrice = Info[2];
                 RetrievalCity = Info[3];
                RetrievalStartTime = Info[4];
                 RetrievalCourier = Info[5];
                RetrievalCourierGender = Info[6];
                 RetrievalCourierPhoneNumber = Info[7];
                 RetrievalCourierAge = Info[8];
                 DeliveryFinishTime = Info[9];
                 DeliveryCity = Info[10];
                 DeliveryCourier = Info[11];
                 DeliveryCourierGender = Info[12];
                 DeliveryCourierPhoneNumber = Info[13];
                 DeliveryCourierAge = Info[14];
                 ItemExportCity = Info[15];
                 ItemExportTax = Info[16];
                 ItemExportTime = Info[17];
                 ItemImportCity = Info[18];
                 ItemImportTax = Info[19];
                 ItemImportTime = Info[20];
                 ContainerCode = Info[21];
                 ContainerType = Info[22];
                 ShipName = Info[23];
                 CompanyName = Info[24];
                 LogTime = Info[25];

                company.setString(1,CompanyName);
                company.addBatch();
                cityR.setString(1,RetrievalCity);
                cityR.addBatch();
                cityD.setString(1,DeliveryCity);
                cityD.addBatch();

                exportCity.setString(1, ItemExportCity);
                exportCity.addBatch();
                importCity.setString(1, ItemImportCity);
                importCity.addBatch();


                shipment.setString(1, ItemName);
                shipment.setFloat(2, Float.parseFloat(ItemPrice));
                shipment.setString(3, ItemType);
                shipment.setString(4, RetrievalCity);
                shipment.setString(5, DeliveryCity);
                shipment.setString(6,ItemExportCity);
                shipment.setString(7,ItemImportCity);
                shipment.setString(8,CompanyName);
                shipment.setTimestamp(9, Timestamp.valueOf(LogTime));
                shipment.addBatch();

                //single insertion



                courierR.setString(1,RetrievalCourier);
                courierR.setString(2,RetrievalCourierGender);
                courierR.setDate(3, CalBirth(RetrievalStartTime, Float.parseFloat(RetrievalCourierAge)));
                courierR.setString(4, RetrievalCourierPhoneNumber);
                courierR.setString(5, CompanyName);
                courierR.setString(6, ItemExportCity);
                courierR.addBatch();

                retrieval.setString(1,ItemName);
                retrieval.setString(2,"retrieval");
                retrieval.setString(3,RetrievalCourier);
                retrieval.setString(4,RetrievalCity);
                retrieval.setDate(5,Date.valueOf(RetrievalStartTime));
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
                    import_detail.setString(1, ItemName);
                    import_detail.setString(2, "import");
                    import_detail.setString(3, ItemType);
                    import_detail.setString(4, ItemImportCity);
                    import_detail.setFloat(5, Float.parseFloat(ItemImportTax));
                    import_detail.setDate(6, Date.valueOf(ItemImportTime));
                    import_detail.addBatch();
                }
                if (!(DeliveryFinishTime.isEmpty())) {
                    courierD.setString(1,DeliveryCourier);
                    courierD.setString(2,DeliveryCourierGender);
                    courierD.setDate(3, CalBirth(DeliveryFinishTime, Float.parseFloat(DeliveryCourierAge)));
                    courierD.setString(4, DeliveryCourierPhoneNumber);
                    courierD.setString(5, CompanyName);
                    courierD.setString(6, ItemImportCity);
                    courierD.addBatch();

                    delivery.setString(1,ItemName);
                    delivery.setString(2,"delivery");
                    delivery.setString(3,DeliveryCourier);
                    delivery.setString(4,DeliveryCity);
                    delivery.setDate(5,Date.valueOf(DeliveryFinishTime));
                    delivery.addBatch();
                }

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
            company.clearBatch();
            exportCity.clearBatch();
            importCity.clearBatch();
            cityR.clearBatch();
            cityD.clearBatch();
            shipment.clearBatch();
            container.clearBatch();
            ship.clearBatch();
            courierR.clearBatch();
            courierD.clearBatch();
            import_detail.clearBatch();
            export_detail.clearBatch();
            retrieval.clearBatch();
            delivery.clearBatch();
            shipping.clearBatch();


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

            con.commit();
            con.setAutoCommit(true);

            con.setAutoCommit(true);
            long endTime=System.currentTimeMillis();
            System.out.printf("Inserted: %d records, speed: %.2f records/s\n",MAXRECORD, (float)(MAXRECORD*1e3/(endTime-startTime)));

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

