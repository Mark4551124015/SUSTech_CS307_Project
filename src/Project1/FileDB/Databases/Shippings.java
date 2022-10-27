package Project1.FileDB.Databases;

import Project1.FileDB.Databases.Models.Shipping;
import Project1.FileDB.FileDBManager;

import java.util.ArrayList;

public class Shippings extends BaseModel<Shipping> {
    @Override
    public void initialize() {
        this.data = new ArrayList<>();
    }

    @Override
    public String getDBPath() {
        return "./FileDatabase/shipping.json";
    }

    public Shipping insert(String itemName, String ship, String containerCode) {
        if (select(item -> item.itemName.equals(itemName)).size() > 0) {
            return null;
        }

        if (FileDBManager.getContainers().select(container -> container.code.equals(containerCode)).size() < 1) {
            return null;
        }

        Shipping newRecord = new Shipping(itemName, ship, containerCode);
        this.data.add(newRecord);
        return newRecord;
    }
}
