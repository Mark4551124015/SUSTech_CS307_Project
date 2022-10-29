import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Calendar;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultiThreadLoader {
    private static int threads = 10;
    private static ExecutorService threadPool = Executors.newFixedThreadPool(threads);
    public void loadFromFile(String filePath, int max) throws Exception {
        long startTime = System.currentTimeMillis();
        Connection op = new JDBC().getNewCon();
        Statement operation = op.createStatement();
        op.setAutoCommit(false);
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
        op.commit();
        CountDownLatch latch = new CountDownLatch(threads);
        threadPool.execute(() ->{
            try {
                icompany(filePath,max,new JDBC().getNewCon());
            } catch (Exception e) {
                System.err.println(e);
            } finally {
                latch.countDown();
            }
        });
        threadPool.execute(() ->{
            try {
                icity(filePath,max,new JDBC().getNewCon());
            } catch (Exception e) {
                System.err.println(e);
            } finally {
                latch.countDown();
            }
        });
        threadPool.execute(() ->{
            try {
                icontainer(filePath,max,new JDBC().getNewCon());
            } catch (Exception e) {
                System.err.println(e);
            } finally {
                latch.countDown();
            }
        });
        threadPool.execute(() ->{
            try {
                iportcity(filePath,max,new JDBC().getNewCon());
            } catch (Exception e) {
                System.err.println(e);
            } finally {
                latch.countDown();
            }
        });
        threadPool.execute(() ->{
            try {
                icourier(filePath,max, new JDBC().getNewCon());
            } catch (Exception e) {
                System.err.println(e);
            } finally {
                latch.countDown();
            }
        });

        threadPool.execute(() ->{
            try {
                iie(filePath,max, new JDBC().getNewCon());
            } catch (Exception e) {
                System.err.println(e);
            } finally {
                latch.countDown();
            }
        });
        threadPool.execute(() ->{
            try {
                ishipping(filePath,max, new JDBC().getNewCon());
            } catch (Exception e) {
                System.err.println(e);
            } finally {
                latch.countDown();
            }
        });
        threadPool.execute(() ->{
            try {
                iship(filePath,max, new JDBC().getNewCon());
            } catch (Exception e) {
                System.err.println(e);
            } finally {
                latch.countDown();
            }
        });
        threadPool.execute(() ->{
            try {
                ishipment(filePath,max, new JDBC().getNewCon());
            } catch (Exception e) {
                System.err.println(e);
            } finally {
                latch.countDown();
            }
        });
        threadPool.execute(() ->{
            try {
                idr(filePath,max, new JDBC().getNewCon());
            } catch (Exception e) {
                System.err.println(e);
            } finally {
                latch.countDown();
            }
        });
        latch.await();
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
        op.commit();
        op.setAutoCommit(true);
        long endTime = System.currentTimeMillis();
        threadPool.shutdown();
        System.out.printf("Load using MultiThread Loader: %d records, speed: %.2f records/s\n",max, (float)(max*1e3/(endTime-startTime)));
    }
    public void icompany (String filePath, int max,Connection con) throws Exception {
        int cnt;
        int MAXRECORD = max;
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
        long startTime = System.currentTimeMillis();
        con.setAutoCommit(false);
        PreparedStatement company = con.prepareStatement("insert into company (name) values (?)  on conflict do nothing;");
        //tax cal after insertion
        String[] Info;
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

            company.setString(1,CompanyName);
            company.addBatch();
        }
        company.executeBatch();
        company.clearBatch();
        con.commit();

        long endTime=System.currentTimeMillis();
//        System.out.printf("Inserted: %d records, speed: %.2f records/s\n",max, (float)(max*1e3/(endTime-startTime)));

    }
    public void icity (String filePath,int max, Connection con) throws Exception {
        int cnt;
        int MAXRECORD = max;
        File csv = new File(filePath);
        csv.setReadable(true);
        csv.setWritable(true);
        InputStreamReader isr = null;
        BufferedReader br = null;
        isr = new InputStreamReader(new FileInputStream(csv), StandardCharsets.UTF_8);
        br = new BufferedReader(isr);
        String line = "";
        cnt = 0;
        int index = 1;
        line = br.readLine();
        long startTime = System.currentTimeMillis();
        con.setAutoCommit(false);
        PreparedStatement cityR = con.prepareStatement("insert into city (name) values (?)  on conflict do nothing;");
        PreparedStatement cityD = con.prepareStatement("insert into city (name) values (?)  on conflict do nothing;");
        //tax cal after insertion
        String[] Info;

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


            cityR.setString(1,RetrievalCity);
            cityR.addBatch();
            cityD.setString(1,DeliveryCity);
            cityD.addBatch();
        }

        cityR.executeBatch();
        cityD.executeBatch();

        cityR.clearBatch();
        cityD.clearBatch();
        con.commit();

//        System.out.printf("Inserted: %d records, speed: %.2f records/s\n",max, (float)(max*1e3/(System.currentTimeMillis()-startTime)));
    }
    public void icontainer (String filePath,int max, Connection con) throws Exception {
        int cnt;
        int MAXRECORD = max;
        File csv = new File(filePath);
        csv.setReadable(true);
        csv.setWritable(true);
        InputStreamReader isr = null;
        BufferedReader br = null;
        isr = new InputStreamReader(new FileInputStream(csv), StandardCharsets.UTF_8);
        br = new BufferedReader(isr);
        String line = "";
        cnt = 0;
        int index = 1;
        line = br.readLine();
        long startTime = System.currentTimeMillis();
        con.setAutoCommit(false);
        PreparedStatement container = con.prepareStatement("insert into container (code, type) values (?,?)  on conflict do nothing;");
        //tax cal after insertion
        String[] Info;

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




            if (!(ShipName.isEmpty())) {
                container.setString(1,ContainerCode);
                container.setString(2,ContainerType);
                container.addBatch();
            }
        }

        container.executeBatch();
        container.clearBatch();
        con.commit();

        long endTime=System.currentTimeMillis();



//        System.out.printf("Inserted: %d records, speed: %.2f records/s\n",max, (float)(max*1e3/(endTime-startTime)));

    }
    public void iportcity(String filePath,int max, Connection con) throws Exception {
        int cnt;
        int MAXRECORD = max;
        File csv = new File(filePath);
        csv.setReadable(true);
        csv.setWritable(true);
        InputStreamReader isr = null;
        BufferedReader br = null;
        isr = new InputStreamReader(new FileInputStream(csv), StandardCharsets.UTF_8);
        br = new BufferedReader(isr);
        String line = "";
        cnt = 0;
        int index = 1;
        line = br.readLine();
        long startTime = System.currentTimeMillis();

        con.setAutoCommit(false);

        PreparedStatement exportCity = con.prepareStatement("insert into portcity (name) values (?)  on conflict do nothing;");
        PreparedStatement importCity = con.prepareStatement("insert into portcity (name) values (?)  on conflict do nothing;");

        //tax cal after insertion
        String[] Info;

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


            exportCity.setString(1, ItemExportCity);
            exportCity.addBatch();
            importCity.setString(1, ItemImportCity);
            importCity.addBatch();



            //single insertion


        }

        exportCity.executeBatch();
        importCity.executeBatch();

        exportCity.clearBatch();
        importCity.clearBatch();

        con.commit();

        long endTime=System.currentTimeMillis();
        threadPool.shutdown();

//        System.out.printf("Inserted: %d records, speed: %.2f records/s\n",max, (float)(max*1e3/(endTime-startTime)));

    }
    public void icourier (String filePath,int max, Connection con) throws Exception {
        int cnt;
        int MAXRECORD = max;
        File csv = new File(filePath);
        csv.setReadable(true);
        csv.setWritable(true);
        InputStreamReader isr = null;
        BufferedReader br = null;
        isr = new InputStreamReader(new FileInputStream(csv), StandardCharsets.UTF_8);
        br = new BufferedReader(isr);
        String line = "";
        cnt = 0;
        int index = 1;
        line = br.readLine();
        long startTime = System.currentTimeMillis();

        con.setAutoCommit(false);


        PreparedStatement courierR = con.prepareStatement("insert into courier (name, gender, birthday, phone_number, company, port_city) values (?,?,?,?,?,?)  on conflict do nothing;");
        PreparedStatement courierD = con.prepareStatement("insert into courier (name, gender, birthday, phone_number, company, port_city) values (?,?,?,?,?,?)  on conflict do nothing;");

        //tax cal after insertion
        String[] Info;

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

            //single insertion

            courierR.setString(1,RetrievalCourier);
            courierR.setString(2,RetrievalCourierGender);
            courierR.setDate(3, CalBirth(RetrievalStartTime, Float.parseFloat(RetrievalCourierAge)));
            courierR.setString(4, RetrievalCourierPhoneNumber);
            courierR.setString(5, CompanyName);
            courierR.setString(6, ItemExportCity);
            courierR.addBatch();

            if (!(DeliveryFinishTime.isEmpty())) {
                courierD.setString(1,DeliveryCourier);
                courierD.setString(2,DeliveryCourierGender);
                courierD.setDate(3, CalBirth(DeliveryFinishTime, Float.parseFloat(DeliveryCourierAge)));
                courierD.setString(4, DeliveryCourierPhoneNumber);
                courierD.setString(5, CompanyName);
                courierD.setString(6, ItemImportCity);
                courierD.addBatch();

            }

        }

        courierR.executeBatch();
        courierD.executeBatch();

        courierR.clearBatch();
        courierD.clearBatch();
        con.commit();
        long endTime=System.currentTimeMillis();
//        System.out.printf("Inserted: %d records, speed: %.2f records/s\n",max, (float)(max*1e3/(endTime-startTime)));

    }
    public void iie (String filePath, int max, Connection con) throws Exception {
        int cnt;
        int MAXRECORD = max;
        File csv = new File(filePath);
        csv.setReadable(true);
        csv.setWritable(true);
        InputStreamReader isr = null;
        BufferedReader br = null;
        isr = new InputStreamReader(new FileInputStream(csv), StandardCharsets.UTF_8);
        br = new BufferedReader(isr);
        String line = "";
        cnt = 0;
        int index = 1;
        line = br.readLine();
        long startTime = System.currentTimeMillis();

        con.setAutoCommit(false);


        PreparedStatement import_detail = con.prepareStatement("insert into import_export_detail (item_name,type, item_type, port_city, tax, date) values (?,?,?,?,?,?)  on conflict do nothing;");
        PreparedStatement export_detail = con.prepareStatement("insert into import_export_detail (item_name,type, item_type, port_city, tax, date) values (?,?,?,?,?,?)  on conflict do nothing;");

        //tax cal after insertion
        String[] Info;

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

            //single insertion



            if (!(ItemExportTime.isEmpty())) {
                export_detail.setString(1, ItemName);
                export_detail.setString(2, "export");
                export_detail.setString(3, ItemType);
                export_detail.setString(4, ItemExportCity);
                export_detail.setFloat(5, Float.parseFloat(ItemExportTax));
                export_detail.setDate(6, Date.valueOf(ItemExportTime));
                export_detail.addBatch();
            }

            if (!(ItemImportTime.isEmpty())) {
                import_detail.setString(1, ItemName);
                import_detail.setString(2, "import");
                import_detail.setString(3, ItemType);
                import_detail.setString(4, ItemImportCity);
                import_detail.setFloat(5, Float.parseFloat(ItemImportTax));
                import_detail.setDate(6, Date.valueOf(ItemImportTime));
                import_detail.addBatch();
            }

        }

        import_detail.executeBatch();
        export_detail.executeBatch();
        import_detail.clearBatch();
        export_detail.clearBatch();
        con.commit();
        long endTime=System.currentTimeMillis();


//        System.out.printf("Inserted: %d records, speed: %.2f records/s\n",max, (float)(max*1e3/(endTime-startTime)));

    }
    public void ishipping (String filePath, int max, Connection con) throws Exception {
        int cnt;
        int MAXRECORD = max;
        File csv = new File(filePath);
        csv.setReadable(true);
        csv.setWritable(true);
        InputStreamReader isr = null;
        BufferedReader br = null;
        isr = new InputStreamReader(new FileInputStream(csv), StandardCharsets.UTF_8);
        br = new BufferedReader(isr);
        String line = "";
        cnt = 0;
        int index = 1;
        line = br.readLine();
        long startTime = System.currentTimeMillis();


        con.setAutoCommit(false);


        PreparedStatement shipping = con.prepareStatement("insert into shipping (item_name, ship, container) values (?,?,?)  on conflict do nothing;");

        //tax cal after insertion
        String[] Info;

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





            if (!(ShipName.isEmpty())) {


                shipping.setString(2,ShipName);
                shipping.setString(3,ContainerCode);
            }else {
                shipping.setString(2,null);
                shipping.setString(3,null);
            }
            shipping.setString(1,ItemName);
            shipping.addBatch();
        }

        shipping.executeBatch();

        shipping.clearBatch();
        con.commit();
        long endTime=System.currentTimeMillis();
//        System.out.printf("Inserted: %d records, speed: %.2f records/s\n",max, (float)(max*1e3/(endTime-startTime)));

    }
    public void iship (String filePath, int max, Connection con) throws Exception {
        int cnt;
        int MAXRECORD = max;
        File csv = new File(filePath);
        csv.setReadable(true);
        csv.setWritable(true);
        InputStreamReader isr = null;
        BufferedReader br = null;
        isr = new InputStreamReader(new FileInputStream(csv), StandardCharsets.UTF_8);
        br = new BufferedReader(isr);
        String line = "";
        cnt = 0;
        int index = 1;
        line = br.readLine();
        long startTime = System.currentTimeMillis();

        con.setAutoCommit(false);

        PreparedStatement ship = con.prepareStatement("insert into ship (name, company) values (?,?)  on conflict do nothing;");

        //tax cal after insertion
        String[] Info;

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



            if (!(ShipName.isEmpty())) {


                ship.setString(1,ShipName);
                ship.setString(2,CompanyName);
                ship.addBatch();

            }


        }

        ship.executeBatch();
        ship.clearBatch();
        con.commit();
        long endTime=System.currentTimeMillis();

//        System.out.printf("Inserted: %d records, speed: %.2f records/s\n",max, (float)(max*1e3/(endTime-startTime)));

    }
    public void ishipment (String filePath, int max, Connection con) throws Exception {
        int cnt;
        int MAXRECORD = max;
        File csv = new File(filePath);
        csv.setReadable(true);
        csv.setWritable(true);
        InputStreamReader isr = null;
        BufferedReader br = null;
        isr = new InputStreamReader(new FileInputStream(csv), StandardCharsets.UTF_8);
        br = new BufferedReader(isr);
        String line = "";
        cnt = 0;
        int index = 1;
        line = br.readLine();
        long startTime = System.currentTimeMillis();
        con.setAutoCommit(false);
        PreparedStatement shipment = con.prepareStatement("insert into shipment (item_name, item_price, item_type, from_city, to_city, export_city, import_city, company, log_time) values (?,?,?,?,?,?,?,?,?)  on conflict do nothing;");
        //tax cal after insertion
        String[] Info;
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
            shipment.setFloat(2, Float.parseFloat(ItemPrice));
            shipment.setString(3, ItemType);
            shipment.setString(4, RetrievalCity);
            shipment.setString(5, DeliveryCity);
            shipment.setString(6,ItemExportCity);
            shipment.setString(7,ItemImportCity);
            shipment.setString(8,CompanyName);
            shipment.setTimestamp(9, Timestamp.valueOf(LogTime));
            shipment.addBatch();



        }
        shipment.executeBatch();
        shipment.clearBatch();
        con.commit();
        long endTime=System.currentTimeMillis();
//        System.out.printf("Inserted: %d records, speed: %.2f records/s\n",max, (float)(max*1e3/(endTime-startTime)));

    }
    public void idr (String filePath, int max, Connection con) throws Exception {
        int cnt;
        int MAXRECORD = max;
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
            con.setAutoCommit(false);
            long startTime=System.currentTimeMillis();
            PreparedStatement retrieval = con.prepareStatement("insert into delivery_retrieval (item_name,type,courier,city,date) values (?,?,?,?,?)  on conflict do nothing;");
            PreparedStatement delivery = con.prepareStatement("insert into delivery_retrieval (item_name,type,courier,city,date) values (?,?,?,?,?)  on conflict do nothing;");
            //tax cal after insertion
            String[] Info;
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



                //single insertion



                retrieval.setString(1,ItemName);
                retrieval.setString(2,"retrieval");
                retrieval.setString(3,RetrievalCourier);
                retrieval.setString(4,RetrievalCity);
                retrieval.setDate(5,Date.valueOf(RetrievalStartTime));
                retrieval.addBatch();

                if (!(DeliveryFinishTime.isEmpty())) {

                    delivery.setString(1,ItemName);
                    delivery.setString(2,"delivery");
                    delivery.setString(3,DeliveryCourier);
                    delivery.setString(4,DeliveryCity);
                    delivery.setDate(5,Date.valueOf(DeliveryFinishTime));
                    delivery.addBatch();
                }
            }
            retrieval.executeBatch();
            delivery.executeBatch();
            retrieval.clearBatch();
            delivery.clearBatch();
            con.commit();
            long endTime=System.currentTimeMillis();
//            System.out.printf("Inserted: %d records, speed: %.2f records/s\n",MAXRECORD, (float)(MAXRECORD*1e3/(endTime-startTime)));
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