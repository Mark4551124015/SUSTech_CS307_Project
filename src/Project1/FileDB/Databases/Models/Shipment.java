package Project1.FileDB.Databases.Models;

import Project1.FileDB.FileDBManager;
import com.google.gson.annotations.Expose;

import java.util.ArrayList;
import java.util.Date;

public class Shipment {

    public String itemName;
    public double itemPrice;
    public String itemType;
    public String fromCity;
    public String toCity;
    public Date logTime;
    public String company;

    @Expose(serialize = false, deserialize = false)
    public ImportAndExport importDetail;
    public ImportAndExport exportDetail;

    public Shipment(String itemName, double itemPrice, String itemType, String fromCity, String toCity, Date logTime, String company) {
        this.itemName = itemName;
        this.itemPrice = itemPrice;
        this.itemType = itemType;
        this.fromCity = fromCity;
        this.toCity = toCity;
        this.logTime = logTime;
        this.company = company;
    }

    public ImportAndExport getImportDetail() {
        if (importDetail == null) {
            ArrayList<ImportAndExport> cache = FileDBManager.getImportAndExports().select(importDetail -> importDetail.itemName.equals(itemName) && importDetail.type.equals("import"));
            if (cache.size() > 0)
                importDetail = cache.get(0);
            else
                importDetail = null;
        }

        return importDetail;
    }

    public ImportAndExport getExportDetail() {
        if (exportDetail == null) {
            ArrayList<ImportAndExport> cache = FileDBManager.getImportAndExports().select(exportDetail -> exportDetail.itemName.equals(itemName) && exportDetail.type.equals("export"));
            if (cache.size() > 0)
                exportDetail = cache.get(0);
            else
                exportDetail = null;
        }

        return exportDetail;
    }
}
