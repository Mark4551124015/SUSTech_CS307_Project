package FileDB.Tables.Models;

import java.util.Date;

public class ImportAndExport {
    public int id;
    public String itemName;
    public String type;
    public String portCity;
    public double tax;
    public Date date;

    public ImportAndExport(int id, String itemName, String type, String portCity, double tax, Date date) {
        this.id = id;
        this.itemName = itemName;
        this.type = type;
        this.portCity = portCity;
        this.tax = tax;
        this.date = date;
    }
}
