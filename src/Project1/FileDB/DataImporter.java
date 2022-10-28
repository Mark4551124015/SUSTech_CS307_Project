package Project1.FileDB;

import Project1.FileDB.Databases.Models.Company;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DataImporter {
    FileDBManager fileDBManager;

    public static void main(String[] args) {
        String sourcePath = "./data/shipment_records.csv";

        DataImporter dataImporter = new DataImporter();
        try {
            dataImporter.importCSV(sourcePath, 2000);
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
            // Timer
            long startTime=System.currentTimeMillis();

            String[] primaryData = line.split(",", -1);
            String ItemName = primaryData[0];
            String ItemType = primaryData[1];
            double ItemPrice = Double.parseDouble(primaryData[2]);
            String RetrievalCity = primaryData[3];
            String RetrievalStartTime = primaryData[4];
            String RetrievalCourier = primaryData[5];
            String RetrievalCourierGender = primaryData[6];
            String RetrievalCourierPhoneNumber = primaryData[7];
            float RetrievalCourierAge = Float.parseFloat(primaryData[8]);
            String DeliveryFinishTime = primaryData[9];
            String DeliveryCity = primaryData[10];
            String DeliveryCourier = primaryData[11];
            String DeliveryCourierGender = primaryData[12];
            String DeliveryCourierPhoneNumber = primaryData[13];
            float DeliveryCourierAge = Float.parseFloat(primaryData[14].isEmpty() ? "0" : primaryData[14]);
            String ItemExportCity = primaryData[15];
            double ItemExportTax = Double.parseDouble(primaryData[16]);
            String ItemExportTime = primaryData[17];
            String ItemImportCity = primaryData[18];
            double ItemImportTax = Double.parseDouble(primaryData[19]);
            String ItemImportTime = primaryData[20];
            String ContainerCode = primaryData[21];
            String ContainerType = primaryData[22];
            String ShipName = primaryData[23];
            String CompanyName = primaryData[24];
            String LogTime = primaryData[25];


            /*
             * Import Start
             */
            importCompanyIfNotExist(CompanyName);
            importCityIfNotExist(DeliveryCity);
            importCityIfNotExist(RetrievalCity);
            importPortCityIfNotExist(ItemImportCity);
            importPortCityIfNotExist(ItemExportCity);
            importCourierIfNotExist(RetrievalCourier, RetrievalCourierGender, CompanyName, RetrievalCity, RetrievalCourierPhoneNumber, RetrievalCourierAge, RetrievalStartTime);
            importRetrieval(ItemName, RetrievalCourier, RetrievalStartTime);

            importShipmentIfNotExist(ItemName, ItemPrice, ItemType, RetrievalCity, DeliveryCity, LogTime);

            if (!DeliveryCourier.isEmpty()) {
                importCourierIfNotExist(DeliveryCourier, DeliveryCourierGender, CompanyName, DeliveryCity, DeliveryCourierPhoneNumber, DeliveryCourierAge, DeliveryFinishTime);
                importDelivery(ItemName, DeliveryCourier, DeliveryFinishTime);
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

            /*
             * Import End
             */

            // To mimic real database, save each time inserted new records
            fileDBManager.save();

            // Timer
            long endTime=System.currentTimeMillis();
            System.out.printf("Inserted %dth record, cost: %d ms\n",counter + 1, (endTime-startTime));


            // prepare for next loop
            line = reader.readLine();
            counter++;
        }
        fileDBManager.save();


        return counter;
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
        FileDBManager.getCouriers().insert(name, gender, birthday, phoneNumber, city);
    }

    public void importShipmentIfNotExist(String itemName, double itemPrice, String itemType, String fromCity, String toCity, String logTimeString) {
        Date logTime = getDatetime(logTimeString);
        FileDBManager.getShipments().insert(itemName, itemPrice, itemType, fromCity, toCity, logTime);
    }

    public void importImport(String itemName, String portCity, double tax, String dateString) {
        Date date = getDate(dateString);
        FileDBManager.getImportAndExports().insert(itemName, "import", portCity, tax, date);
    }

    public void importExport(String itemName, String portCity, double tax, String dateString) {
        Date date = getDate(dateString);
        FileDBManager.getImportAndExports().insert(itemName, "export", portCity, tax, date);
    }

    public void importDelivery(String itemName, String courier, String dateString) {
        Date date = getDate(dateString);
        FileDBManager.getDeliveryAndRetrievals().insert(itemName, courier, "delivery", date);
    }

    public void importRetrieval(String itemName, String courier, String dateString) {
        Date date = getDate(dateString);
        FileDBManager.getDeliveryAndRetrievals().insert(itemName, courier, "retrieval", date);
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
            return new SimpleDateFormat("yyyy-mm-dd").parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Date getDatetime(String str) {
        try {
            return new SimpleDateFormat("yyyy-mm-dd HH:mm:ss").parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
}
