package FileDB;

import FileDB.Tables.Models.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * Advanced Queries For The First Three Advance Requirements
 */
public class AdvancedQuery {
    FileDBManager fileDBManager;

    public static void main(String[] args) {
        AdvancedQuery advancedQuery = new AdvancedQuery();
        advancedQuery.queryContainersWithWorkingTime(150, "Dry Container");


//        advancedQuery.queryBestCourier("聚划算", "北京");
//        advancedQuery.queryBestCourier("聚划算", "成都");
//        advancedQuery.queryBestExportCity("apple");
//        advancedQuery.queryBestExportCity("mango");
    }

    public AdvancedQuery() {
        fileDBManager = FileDBManager.getInstance();
    }

    /**
     * Query the containers of specific type which work more than the specific days
     * @param minDays
     * @param type
     */
    public void queryContainersWithWorkingTime(int minDays, String type) {
        long startTime = System.currentTimeMillis();
        ArrayList<Container> containers = FileDBManager.getContainers().select(container -> container.type.equals(type));
        for (Container container : containers) {
            ArrayList<Shipping> shippings = FileDBManager.getShippings().select(shipping -> shipping.containerCode.equals(container.code));
            HashSet<String> itemNames = new HashSet<>();
            long serviceDays = 0;

            for (Shipping shipping : shippings) {
                itemNames.add(shipping.itemName);
            }
            ArrayList<Shipment> shipments = FileDBManager.getShipments().select(shipment ->
                    itemNames.contains(shipment.itemName) && shipment.getExportDetail() != null && shipment.getImportDetail() != null
            );

            for (Shipment shipment : shipments) {
                Date importDate = shipment.getImportDetail().date;
                Date exportDate = shipment.getExportDetail().date;
                serviceDays += dateDiff(exportDate, importDate);
            }
            if (serviceDays >= minDays){
                System.out.printf("Container %s (type: %s) has worked for %d days \n", container.code, container.type, serviceDays);
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.printf("All queries are finished, total cost: %d ms \n", endTime - startTime);
    }

    /**
     * Query all the containers with their types and working time.
     */
    public void queryContainersWithWorkingTime() {
        long startTime = System.currentTimeMillis();
        ArrayList<Container> containers = FileDBManager.getContainers().select(container -> true);
        for (Container container : containers) {
            ArrayList<Shipping> shippings = FileDBManager.getShippings().select(shipping -> shipping.containerCode.equals(container.code));
            HashSet<String> itemNames = new HashSet<>();
            long serviceDays = 0;

            for (Shipping shipping : shippings) {
                itemNames.add(shipping.itemName);
            }
            ArrayList<Shipment> shipments = FileDBManager.getShipments().select(shipment ->
                    itemNames.contains(shipment.itemName) && shipment.getExportDetail() != null && shipment.getImportDetail() != null
            );

            for (Shipment shipment : shipments) {
                Date importDate = shipment.getImportDetail().date;
                Date exportDate = shipment.getExportDetail().date;
                serviceDays += dateDiff(exportDate, importDate);
            }

            System.out.printf("Container %s (type: %s) has worked for %d days \n", container.code, container.type, serviceDays);
        }
        long endTime = System.currentTimeMillis();
        long cost = TimeUnit.SECONDS.convert(endTime - startTime, TimeUnit.MILLISECONDS);
        System.out.printf("All queries are finished, total cost: %d seconds \n", cost);
    }

    /**
     * Query the couriers who delivery and retrieve most for the
     * specific company in the specific city
     */
    public void queryBestCourier(String company, String city) {
        long startTime = System.currentTimeMillis();
        ArrayList<Courier> couriers = FileDBManager.getCouriers().select(courier -> true);


        int maxDeliveryCount = 0;
        int maxRetrievalCount = 0;
        Courier bestDeliveryCourier = null;
        Courier bestRetrievalCourier = null;

        for (Courier courier : couriers) {
            ArrayList<DeliveryAndRetrieval> deliveries = FileDBManager.getDeliveryAndRetrievals().select(delivery ->
                    delivery.city != null && delivery.city.equals(city) && delivery.courier.equals(courier.name) && delivery.type.equals("delivery")
            );

            ArrayList<DeliveryAndRetrieval> retrievals = FileDBManager.getDeliveryAndRetrievals().select(retrieval ->
                    retrieval.city != null && retrieval.city.equals(city) && retrieval.courier.equals(courier.name) && retrieval.type.equals("retrieval")
            );
            if (deliveries.size() > maxDeliveryCount) {
                maxDeliveryCount = deliveries.size();
                bestDeliveryCourier = courier;
            }
            if (retrievals.size() > maxRetrievalCount) {
                maxRetrievalCount = retrievals.size();
                bestRetrievalCourier = courier;
            }
        }

        if (maxRetrievalCount != 0) {
            System.out.printf("The best retrieval courier of company %s in %s is %s\n", company, city, bestRetrievalCourier.name);
        }
        if (maxDeliveryCount != 0) {
            System.out.printf("The best delivery  courier of company %s in %s is %s\n", company, city, bestDeliveryCourier.name);
        }

        long endTime = System.currentTimeMillis();
        System.out.printf("All queries are finished, total cost: %d ms \n", endTime - startTime);
    }

    /**
     * Query the city which has the lowest export tax rate with respect to the specific itemType
     * @param itemType
     */

    public void queryBestExportCity(String itemType) {
        long startTime = System.currentTimeMillis();
        HashSet<String> itemTypes = new HashSet<>();
        ArrayList<Shipment> shipments = FileDBManager.getShipments().select(shipment -> true);
        for (Shipment shipment : shipments) {
            itemTypes.add(shipment.itemType);
        }

        ArrayList<ImportAndExport> exports = FileDBManager.getImportAndExports().select(export ->
                export.type.equals("export") && export.itemName.startsWith(itemType));

        double minExportRate = 100;
        String bestExportCity = null;
        for (ImportAndExport export : exports) {
            double price = FileDBManager.getShipments().select(shipment -> shipment.itemName.equals(export.itemName)).get(0).itemPrice;
            double rate = export.tax / price;

            if (rate < minExportRate) {
                minExportRate = rate;
                bestExportCity = export.portCity;
            }
        }

        System.out.printf("The best export city for %s is %s \n", itemType, bestExportCity);


        long endTime = System.currentTimeMillis();
        System.out.printf("All queries are finished, total cost: %d ms \n", endTime - startTime);
    }

    public void queryBestExportCity() {
        long startTime = System.currentTimeMillis();
        HashSet<String> itemTypes = new HashSet<>();
        ArrayList<Shipment> shipments = FileDBManager.getShipments().select(shipment -> true);
        for (Shipment shipment : shipments) {
            itemTypes.add(shipment.itemType);
        }

        for (String itemType : itemTypes) {
            ArrayList<ImportAndExport> exports = FileDBManager.getImportAndExports().select(export ->
                    export.type.equals("export") && export.itemName.startsWith(itemType));

            double minExportRate = 100;
            String bestExportCity = null;
            for (ImportAndExport export : exports) {
                double price = FileDBManager.getShipments().select(shipment -> shipment.itemName.equals(export.itemName)).get(0).itemPrice;
                double rate = export.tax / price;

                if (rate < minExportRate) {
                    minExportRate = rate;
                    bestExportCity = export.portCity;
                }
            }

            System.out.printf("The best export city for %s is %s \n", itemType, bestExportCity);
        }


        long endTime = System.currentTimeMillis();
        long cost = TimeUnit.SECONDS.convert(endTime - startTime, TimeUnit.MILLISECONDS);
        System.out.printf("All queries are finished, total cost: %d seconds", cost);
    }

    public long dateDiff(Date smallDate, Date bigDate) {
        long diffInMS = bigDate.getTime() - smallDate.getTime();
        return TimeUnit.DAYS.convert(diffInMS, TimeUnit.MILLISECONDS);
    }
}
