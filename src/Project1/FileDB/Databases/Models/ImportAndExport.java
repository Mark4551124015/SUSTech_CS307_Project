package Project1.FileDB.Databases.Models;

import java.util.Date;

public class ImportAndExport {
    public int port_id;
    public String type;
    public double tax;
    public Date date;

    public ImportAndExport(int port_id, String type, double tax, Date date) {
        this.port_id = port_id;
        this.type = type;
        this.tax = tax;
        this.date = date;
    }
}
