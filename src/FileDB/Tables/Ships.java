package FileDB.Tables;

import FileDB.Tables.Models.Ship;
import FileDB.FileDBManager;

import java.util.ArrayList;

public class Ships extends BaseTable<Ship> {

    public void initialize() {
        this.data = new ArrayList<>();
    }

    @Override
    public String getDBPath() {
        return "./FileDatabase/ship.json";
    }

/*    public Ship[] select(Constraint<Ship> constraint) {
        ArrayList<Ship> result = new ArrayList<>();
        for (Ship ship : ships) {
            if (constraint.check(ship)) {
                result.add(ship);
            }
        }
        return result.toArray(Ship[]::new);
    }*/

    public Ship insert(String name, String companyName) {
        if (FileDBManager.getCompanies().select(company -> company.name.equals(companyName)).size() < 1) {
            return null;
        }

        if (select(ship -> ship.name.equals(name)).size() > 0) {
            return null;
        }

        Ship newRecord = new Ship(name, companyName);

        data.add(newRecord);
        return newRecord;
    }
}
