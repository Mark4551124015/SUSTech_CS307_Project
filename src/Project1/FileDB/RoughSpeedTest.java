package Project1.FileDB;

import Project1.FileDB.Databases.Models.Courier;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class RoughSpeedTest {
    static int importDataNumber = 20000;
    static int insertNumber = 10000;
    static int updateNumber = 10000;
    static int deleteNumber = 10000;
    static int queryNumber = 10000;
    static FileDBManager fileDBManager;
    static String sourcePath = "./data/shipment_records.csv";

    public static void main(String[] args) {
        RoughSpeedTest speedTest = new RoughSpeedTest();
        try {
            speedTest.insertTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public RoughSpeedTest() {
        fileDBManager = FileDBManager.getInstance();
    }

    public void insertTest() throws IOException {
        FileDBManager.initializeAll();

        File csvFile = new File(sourcePath);
        DataImporter dataImporter = new DataImporter();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(csvFile), StandardCharsets.UTF_8));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        reader.readLine(); // remove table head
        int counter = 0;
        String nextLine = reader.readLine(); // remove table head
        long startTime = System.currentTimeMillis();
        while (nextLine != null && counter < insertNumber) {
            // Timer

            String[] piece = nextLine.split(",", -1);
            dataImporter.insertPiece(piece);



            counter++;
            nextLine = reader.readLine();
        }
        fileDBManager.save();

        // Timer
        long endTime = System.currentTimeMillis();
        System.out.printf("Inserted %d records, speed: %.2f records/s", counter, (float)insertNumber/(endTime - startTime));
    }

    public void updateTest() throws IOException {
        FileDBManager.initializeAll();
        importData();

        File csvFile = new File(sourcePath);
        PrintWriter out = new PrintWriter(System.out);

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(csvFile), StandardCharsets.UTF_8));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        reader.readLine(); // remove table head
        int counter = 0;
        String nextLine = reader.readLine(); // remove table head

        Courier templateCourier = new Courier(null, null, null, "1008611", null, null);
        // Timer
        long startTime = System.currentTimeMillis();
        while (nextLine != null && counter < updateNumber) {


            String[] piece = nextLine.split(",", -1);
            String RetrievalCourier = piece[5];
            String DeliveryCourier = piece[11];
            FileDBManager.getCouriers().update(courier -> (courier.name.equals(RetrievalCourier) || courier.name.equals(DeliveryCourier)), templateCourier);

            counter++;
            nextLine = reader.readLine();
        }
        FileDBManager.getCouriers().save();
        // Timer
        long endTime = System.currentTimeMillis();
        System.out.printf("Updated %d records, speed: %.2f records/s", counter, (float)insertNumber/(endTime - startTime));

    }

    public void deleteTest() throws IOException {
        FileDBManager.initializeAll();
        importData();

        File csvFile = new File(sourcePath);

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(csvFile), StandardCharsets.UTF_8));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        reader.readLine(); // remove table head
        int counter = 0;
        String nextLine = reader.readLine(); // remove table head
        // Timer
        long startTime = System.currentTimeMillis();
        while (nextLine != null && counter < deleteNumber) {


            String[] piece = nextLine.split(",", -1);
            String ItemName = piece[0];
            FileDBManager.getShipments().delete(shipment -> shipment.itemName.equals(ItemName));
            FileDBManager.getDeliveryAndRetrievals().delete(item -> item.itemName.equals(ItemName));
            FileDBManager.getImportAndExports().delete(item -> item.itemName.equals(ItemName));


            counter++;
            nextLine = reader.readLine();
        }

        FileDBManager.getShipments().save();
        FileDBManager.getDeliveryAndRetrievals().save();
        FileDBManager.getImportAndExports().save();

        // Timer
        long endTime = System.currentTimeMillis();
        System.out.printf("Deleted %d records, speed: %.2f records/s", counter, (float)insertNumber/(endTime - startTime));
    }

    public void queryTest() throws IOException {
        FileDBManager.initializeAll();
        importData();

        File csvFile = new File(sourcePath);

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(csvFile), StandardCharsets.UTF_8));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        reader.readLine(); // remove table head
        int counter = 0;
        String nextLine = reader.readLine(); // remove table head

        long startTime = System.currentTimeMillis();
        while (nextLine != null && counter < queryNumber) {

            String[] piece = nextLine.split(",", -1);
            String ItemName = piece[0];
            FileDBManager.getShipments().select(shipment -> shipment.itemName.equals(ItemName));


            counter++;
            nextLine = reader.readLine();
        }
        // Timer
        long endTime = System.currentTimeMillis();
        System.out.printf("Queried %d records, speed: %.2f records/s", counter, (float)insertNumber/(endTime - startTime));

    }

    public void importData() {
        DataImporter dataImporter = new DataImporter();
        try {
            dataImporter.importCSV(sourcePath, importDataNumber);

            // mimic real database
            FileDBManager.shuffleAll();
            FileDBManager.save();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
