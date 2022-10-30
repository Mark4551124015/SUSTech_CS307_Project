package FileDB;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Data Importer of File DB
 *
 * Run it directly to import data into File DB
 * Foreign key check is automatically enabled
 */
public class DataImporter {
    FileDBManager fileDBManager;
    public static void main(String[] args) {
        /**
         * Change the path here to specific source data file
         * Change MaxRecord to set limit for this import
         */
        String sourcePath = "./data/shipment_records.csv";
        int MaxRecord = 50000;

        DataImporter dataImporter = new DataImporter();
        try {

            dataImporter.importCSV(sourcePath, MaxRecord);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public DataImporter() {
        // initialize fileDBManager;
        fileDBManager = FileDBManager.getInstance();
    }

    public int importCSV(String path, int max) throws IOException {
        File csvFile = new File(path);
        if (!csvFile.exists() || !csvFile.canRead()) {
            return 0;
        }
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(csvFile), StandardCharsets.UTF_8));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        reader.readLine(); // dispose table head
        String line = reader.readLine();
        int counter = 0;

        while (line != null && counter < max) {
/*
            // Timer
            long startTime = System.currentTimeMillis();
*/

            String[] piece = line.split(",", -1);
            insertPiece(piece);

            /*
             * Import End
             */

/*            // Timer
            long endTime = System.currentTimeMillis();
            System.out.printf("Inserted %dth record, cost: %d ms\n", counter + 1, (endTime - startTime));*/

            // prepare for next loop
            line = reader.readLine();
            counter++;
        }
        fileDBManager.save();


        return counter;
    }

    public void insertPiece(String[] piece) {
        String ItemName = piece[0];
        String ItemType = piece[1];
        double ItemPrice = Double.parseDouble(piece[2]);
        String RetrievalCity = piece[3];
        String RetrievalStartTime = piece[4];
        String RetrievalCourier = piece[5];
        String RetrievalCourierGender = piece[6];
        String RetrievalCourierPhoneNumber = piece[7];
        float RetrievalCourierAge = Float.parseFloat(piece[8]);
        String DeliveryFinishTime = piece[9];
        String DeliveryCity = piece[10];
        String DeliveryCourier = piece[11];
        String DeliveryCourierGender = piece[12];
        String DeliveryCourierPhoneNumber = piece[13];
        float DeliveryCourierAge = Float.parseFloat(piece[14].isEmpty() ? "0" : piece[14]);
        String ItemExportCity = piece[15];
        double ItemExportTax = Double.parseDouble(piece[16]);
        String ItemExportTime = piece[17];
        String ItemImportCity = piece[18];
        double ItemImportTax = Double.parseDouble(piece[19]);
        String ItemImportTime = piece[20];
        String ContainerCode = piece[21];
        String ContainerType = piece[22];
        String ShipName = piece[23];
        String CompanyName = piece[24];
        String LogTime = piece[25];


        /*
         * Import Start
         */
        importCompanyIfNotExist(CompanyName);
        importCityIfNotExist(DeliveryCity);
        importCityIfNotExist(RetrievalCity);
        importPortCityIfNotExist(ItemImportCity);
        importPortCityIfNotExist(ItemExportCity);
        importCourierIfNotExist(RetrievalCourier, RetrievalCourierGender, CompanyName, ItemExportCity, RetrievalCourierPhoneNumber, RetrievalCourierAge, RetrievalStartTime);
        importRetrieval(ItemName, RetrievalCourier, RetrievalStartTime, RetrievalCity);

        importShipmentIfNotExist(ItemName, ItemPrice, ItemType, RetrievalCity, DeliveryCity, LogTime, CompanyName);

        if (!DeliveryCourier.isEmpty()) {
            importCourierIfNotExist(DeliveryCourier, DeliveryCourierGender, CompanyName, ItemImportCity, DeliveryCourierPhoneNumber, DeliveryCourierAge, DeliveryFinishTime);
            importDelivery(ItemName, DeliveryCourier, DeliveryFinishTime, DeliveryCity);
        }

        if (!ItemExportTime.isEmpty()) {
            importContainerIfNotExist(ContainerCode, ContainerType);
            importShipIfNotExist(ShipName, CompanyName);

            importExport(ItemName, ItemImportCity, ItemExportTax, ItemExportTime);
            importShipping(ItemName, ShipName, ContainerCode);
        }

        if (!ItemImportTime.isEmpty()) {
            importImport(ItemName, ItemImportCity, ItemImportTax, ItemImportTime);
        }
    }

    public void importCompanyIfNotExist(String companyName) {
        FileDBManager.getCompanies().insert(companyName);
    }

    public void importShipIfNotExist(String shipName, String company) {
        FileDBManager.getShips().insert(shipName, company);
    }

    public void importCityIfNotExist(String cityName) {
        FileDBManager.getCities().insert(cityName);
    }

    public void importPortCityIfNotExist(String cityName) {
        FileDBManager.getPortCities().insert(cityName);
    }

    public void importContainerIfNotExist(String containerCode, String type) {
        FileDBManager.getContainers().insert(containerCode, type);
    }

    public void importCourierIfNotExist(String name, String gender, String company, String city, String phoneNumber, float age, String currentDate) {
        Date birthday = getBirthDay(currentDate, age);
        FileDBManager.getCouriers().insert(name, gender, birthday, phoneNumber, city, company);
    }

    public void importShipmentIfNotExist(String itemName, double itemPrice, String itemType, String fromCity, String toCity, String logTimeString, String company) {
        Date logTime = getDatetime(logTimeString);
        FileDBManager.getShipments().insert(itemName, itemPrice, itemType, fromCity, toCity, logTime, company);
    }

    public void importImport(String itemName, String portCity, double tax, String dateString) {
        Date date = getDate(dateString);
        FileDBManager.getImportAndExports().insert(itemName, "import", portCity, tax, date);
    }

    public void importExport(String itemName, String portCity, double tax, String dateString) {
        Date date = getDate(dateString);
        FileDBManager.getImportAndExports().insert(itemName, "export", portCity, tax, date);
    }

    public void importDelivery(String itemName, String courier, String dateString, String city) {
        Date date = getDate(dateString);
        FileDBManager.getDeliveryAndRetrievals().insert(itemName, courier, "delivery", date, city);
    }

    public void importRetrieval(String itemName, String courier, String dateString, String city) {
        Date date = getDate(dateString);
        FileDBManager.getDeliveryAndRetrievals().insert(itemName, courier, "retrieval", date, city);
    }

    public void importShipping(String itemName, String ship, String containerCode) {
        FileDBManager.getShippings().insert(itemName, ship, containerCode);
    }


    /*
    Copy from loader
     */
    public static Date getBirthDay(String str, float age) {
        Date date = getDate(str);
        Calendar birth = Calendar.getInstance();
        birth.setTime(date);
        birth.add(Calendar.YEAR, -1 * (int) age);
        long temp = birth.getTime().getTime();
        return new Date(temp);
    }

    public static Date getDate(String str) {
        try {
            return new SimpleDateFormat("yyyy-MM-dd").parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Date getDatetime(String str) {
        try {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
}
