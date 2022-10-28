package Project1.FileDB.Databases;

import Project1.FileDB.Databases.Models.Constraint;
import Project1.FileDB.Databases.Models.DeliveryAndRetrieval;
import Project1.FileDB.FileDBManager;

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

    public DeliveryAndRetrieval insert(String itemName, String courierName, String type, Date date) {
        if (FileDBManager.getCouriers().select(courier -> courier.name.equals(courierName)).size() < 1) {
            return null;
        }

        if (select(deliveryAndRetrieval -> deliveryAndRetrieval.itemName.equals(itemName) && deliveryAndRetrieval.type.equals(type)).size() > 0) {
            return null;
        }
        DeliveryAndRetrieval newRecord = new DeliveryAndRetrieval(selfIncreasingNumber, itemName, courierName, type, date);
        data.add(newRecord);
        selfIncreasingNumber++;

        return newRecord;
    }
}
