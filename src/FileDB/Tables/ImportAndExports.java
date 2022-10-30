package FileDB.Tables;

import FileDB.Tables.Models.ImportAndExport;
import FileDB.FileDBManager;

import java.util.ArrayList;
import java.util.Date;

public class ImportAndExports extends BaseTable<ImportAndExport> {
    protected int selfIncreasingNumber;

    @Override
    public void initialize() {
        selfIncreasingNumber = 1;
        this.data = new ArrayList<>();
    }

    public ImportAndExport insert(String itemName, String type, String portCityName, double tax, Date date) {
        if (FileDBManager.getShipments().select(shipment -> shipment.itemName.equals(itemName)).size() < 1) {
            return null;
        }
        if (FileDBManager.getPortCities().select(portCity -> portCity.name.equals(portCityName)).size() < 1) {
            return null;
        }
        if (select(importAndExport -> importAndExport.type.equals(type) && importAndExport.itemName.equals(itemName)).size() > 0) {
            return null;
        }

        ImportAndExport newRecord = new ImportAndExport(selfIncreasingNumber, itemName, type, portCityName, tax, date);
        this.data.add(newRecord);
        selfIncreasingNumber++;
        return newRecord;

    }

    @Override
    public String getDBPath() {
        return "./FileDatabase/import-and-export.json";
    }
}
