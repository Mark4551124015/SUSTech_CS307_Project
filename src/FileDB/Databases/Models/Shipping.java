package FileDB.Databases.Models;

public class Shipping {
    public String itemName;
    public String ship;
    public String containerCode;

    public Shipping(String itemName, String ship, String containerCode) {
        this.itemName = itemName;
        this.ship = ship;
        this.containerCode = containerCode;
    }
}
