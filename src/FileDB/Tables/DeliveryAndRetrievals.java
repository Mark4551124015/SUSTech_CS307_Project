package FileDB.Tables;

import FileDB.Tables.Models.DeliveryAndRetrieval;
import FileDB.FileDBManager;

import java.util.ArrayList;
import java.util.Date;

public class DeliveryAndRetrievals extends BaseModel<DeliveryAndRetrieval> {
    protected int selfIncreasingNumber;

    @Override
    public void initialize() {
        this.data = new ArrayList<>();
        selfIncreasingNumber = 1;
    }

    @Override
    public String getDBPath() {
        return "./FileDatabase/delivery-and-retrieval.json";
    }

/*    public DeliveryAndRetrieval[] select(Constraint<DeliveryAndRetrieval> constraint) {
        ArrayList<DeliveryAndRetrieval> result = new ArrayList<>();
        for (DeliveryAndRetrieval deliveryAndRetrieval : data) {
            if (constraint.check(deliveryAndRetrieval)) {
                result.add(deliveryAndRetrieval);
            }
        }

        return result.toArray(DeliveryAndRetrieval[]::new);
    }*/

    public DeliveryAndRetrieval insert(String itemName, String courierName, String type, Date date, String cityName) {
        if (FileDBManager.getCouriers().select(courier -> courier.name.equals(courierName)).size() < 1) {
            return null;
        }

        if (select(deliveryAndRetrieval -> deliveryAndRetrieval.itemName.equals(itemName) && deliveryAndRetrieval.type.equals(type)).size() > 0) {
            return null;
        }

        if (FileDBManager.getCities().select(city -> city.name.equals(cityName)).size() < 1) {
            return null;
        }

        DeliveryAndRetrieval newRecord = new DeliveryAndRetrieval(selfIncreasingNumber, itemName, courierName, type, date, cityName);
        data.add(newRecord);
        selfIncreasingNumber++;

        return newRecord;
    }
}
