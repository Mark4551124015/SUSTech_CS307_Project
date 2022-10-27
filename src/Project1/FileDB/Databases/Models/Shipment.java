package Project1.FileDB.Databases.Models;

import java.util.Date;

public class Shipment {

    public String itemName;
    public double itemPrice;
    public String itemType;
    public String fromCity;
    public String toCity;
    public Date logTime;

    public Shipment(String itemName, double itemPrice, String itemType, String fromCity, String toCity, Date logTime) {
        this.itemName = itemName;
        this.itemPrice = itemPrice;
        this.itemType = itemType;
        this.fromCity = fromCity;
        this.toCity = toCity;
        this.logTime = logTime;
    }
}
