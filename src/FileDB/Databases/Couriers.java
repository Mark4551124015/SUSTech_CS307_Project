package FileDB.Databases;

import FileDB.Databases.Models.Constraint;
import FileDB.Databases.Models.Courier;
import FileDB.Databases.Models.PortCity;
import FileDB.FileDBManager;

import java.util.ArrayList;
import java.util.Date;

public class Couriers extends BaseModel<Courier> {

    public void initialize() {
        this.data = new ArrayList<Courier>();
    }

    @Override
    public String getDBPath() {
        return "./FileDatabase/courier.json";
    }

/*    public Courier[] select(Constraint<Courier> constraint) {
        ArrayList<Courier> result = new ArrayList<>();
        for (Courier courier : couriers) {
            if (constraint.check(courier)) {
                result.add(courier);
            }
        }
        return result.toArray(Courier[]::new);
    }*/

    public Courier insert(String name, String gender, Date birthday, String phoneNumber, String portCity, String company) {
        // Check Foreign Key
        if (cityForeignKeyNotExist(portCity)) {
            return null;
        }

        if (select(courier -> courier.name.equals(name)).size() > 0) {
            return null;
        }

        Courier newCourier = new Courier(name, gender, birthday, phoneNumber, portCity, company);
        data.add(newCourier);
        return newCourier;
    }

/*    public boolean delete(Constraint<Courier> constraint) {
        Iterator<Courier> iterator = data.iterator();
        while (iterator.hasNext()) {
            Courier courier = iterator.next();
            if (constraint.check(courier)) {
                iterator.remove();
                return true;
            }
        }

        return false;
    }*/


    public int update(Constraint<Courier> constraint, Courier newModel) {
        int affectedDataCount = 0;

        for (Courier courier : data) {
            if (constraint.check(courier)) {

                // Check Foreign Key
                if (newModel.city != null && cityForeignKeyNotExist(newModel.city)) {
                    continue;
                }

                courier.phoneNumber = newModel.phoneNumber != null ? newModel.phoneNumber : courier.phoneNumber;
                courier.birthday = newModel.birthday != null ? newModel.birthday : courier.birthday;
                courier.gender = newModel.gender != null ? newModel.gender : courier.gender;
                courier.name = newModel.name != null ? newModel.name : courier.name;
                courier.city = newModel.city != null ? newModel.city : courier.city;
                courier.company = newModel.company != null ? newModel.company : courier.company;


                affectedDataCount++;
            }
        }

        return affectedDataCount;
    }

    protected boolean cityForeignKeyNotExist(String cityName) {
        ArrayList<PortCity> cities = FileDBManager.getPortCities().select((city) -> city.name.equals(cityName));
        return cities.size() < 1;

    }


}
