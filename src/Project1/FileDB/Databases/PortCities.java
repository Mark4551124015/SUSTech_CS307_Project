package Project1.FileDB.Databases;

import Project1.FileDB.Databases.Models.PortCity;

import java.util.ArrayList;

public class PortCities extends BaseModel<PortCity> {

    @Override
    public void initialize() {
        data = new ArrayList<>();
    }

    @Override
    public String getDBPath() {
        return "./FileDatabase/port-city.json";
    }

/*    public PortCity[] select(Constraint<PortCity> constraint) {
        ArrayList<PortCity> result = new ArrayList<>();
        for (PortCity item : portCities) {
            if (constraint.check(item)) {
                result.add(item);
            }
        }
        return result.toArray(PortCity[]::new);
    }*/

    public boolean insert(String name) {
        if (select(item -> item.name.equals(name)).size() > 0) {
            return false;
        }
        PortCity newRecord = new PortCity(name);
        data.add(newRecord);

        return true;
    }
}
