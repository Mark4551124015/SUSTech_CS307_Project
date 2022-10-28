package Project1.FileDB.Databases.Models;

import java.util.Date;

public class DeliveryAndRetrieval {
    public int id;
    public String itemName;
    public String courier;
    public String type;
    public Date date;

    public DeliveryAndRetrieval(int id, String itemName, String courier, String type, Date date) {
        this.id = id;
        this.itemName = itemName;
        this.courier = courier;
        this.type = type;
        this.date = date;
    }
}
