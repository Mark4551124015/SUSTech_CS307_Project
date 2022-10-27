package Project1.FileDB.Databases.Models;

import java.util.Date;

public class DeliveryAndRetrieval {
    public int dr_id;
    public String courier;
    public String type;
    public Date date;

    public DeliveryAndRetrieval(int dr_id, String courier, String type, Date date) {
        this.dr_id = dr_id;
        this.courier = courier;
        this.type = type;
        this.date = date;
    }
}
