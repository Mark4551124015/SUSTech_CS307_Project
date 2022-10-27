package Project1.FileDB.Databases;

import Project1.FileDB.Databases.Models.Shipment;
import Project1.FileDB.FileDBManager;

import java.util.ArrayList;
import java.util.Date;

public class Shipments extends BaseModel<Shipment> {
    @Override
    public void initialize() {
        this.data = new ArrayList<>();
    }

    @Override
    public String getDBPath() {
        return "./FileDatabase/shipment.json";
    }

    public Shipment insert(String itemName, double itemPrice, String itemType, String fromCity, String toCity, Date logTime) {
        if (FileDBManager.getShippings().select(shipping -> shipping.itemName.equals(itemName)).size() < 1) {
            return null;
        }

        if (FileDBManager.getCities().select(city -> city.name.equals(fromCity)).size() < 1
                || FileDBManager.getCities().select(city -> city.name.equals(toCity)).size() < 1) {
            return null;
        }

        Shipment newRecord = new Shipment(itemName, itemPrice, itemType, fromCity, toCity, logTime);
        data.add(newRecord);

        return newRecord;
    }
}
