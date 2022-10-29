package FileDB.Databases;

import FileDB.Databases.Models.Container;

import java.util.ArrayList;

public class Containers extends BaseModel<Container> {

    public void initialize() {
        this.data = new ArrayList<Container>();
    }

    @Override
    public String getDBPath() {
        return "./FileDatabase/container.json";
    }

/*    public Container[] select(Constraint<Container> constraint) {
        ArrayList<Container> result = new ArrayList<>();
        for (Container container : data) {
            if (constraint.check(container)) {
                result.add(container);
            }
        }

        return result.toArray(Container[]::new);
    }*/

    public Container insert(String code, String type) {
        if (select(container -> container.code.equals(code)).size() > 0) {
            return null;
        }
        Container newRecord = new Container(code, type);
        data.add(newRecord);
        return newRecord;

    }
}
