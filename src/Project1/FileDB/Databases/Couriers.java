package Project1.FileDB.Databases;

import Project1.FileDB.Databases.Models.City;
import Project1.FileDB.Databases.Models.Constraint;
import Project1.FileDB.Databases.Models.Courier;
import Project1.FileDB.FileDBManager;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Objects;

public class Couriers extends BaseModel<Courier> {
    protected int selfIncreasingNumber;

    public void initialize() {
        this.selfIncreasingNumber = 1;
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

    public Courier insert(String name, String gender, Date birthday, String phoneNumber, String city) {
        // Check Foreign Key
        if (cityForeignKeyNotExist(city)) {
            return null;
        }

        Courier newCourier = new Courier(selfIncreasingNumber, name, gender, birthday, phoneNumber, city);
        selfIncreasingNumber++;
        data.add(newCourier);
        return newCourier;
    }

    public boolean delete(Constraint<Courier> constraint) {
        Iterator<Courier> iterator = data.iterator();
        while (iterator.hasNext()) {
            Courier courier = iterator.next();
            if (constraint.check(courier)) {
                iterator.remove();
                return true;
            }
        }

        return false;
    }


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


                affectedDataCount++;
            }
        }

        return affectedDataCount;
    }

    protected boolean cityForeignKeyNotExist(String cityName) {
        ArrayList<City> cities = FileDBManager.getCities().select((city) -> city.name.equals(cityName));
        return cities.size() < 1;

    }


}
