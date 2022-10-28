package Project1.FileDB.Databases;

import Project1.FileDB.Databases.Models.Constraint;
import Project1.FileDB.FileDBManager;
import com.google.gson.Gson;
import com.google.gson.annotations.Expose;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public abstract class BaseModel<T> {
    public ArrayList<T> data;

    public abstract void initialize();

    public File getDBFile() {
        return new File(getDBPath());
    }

    public abstract String getDBPath();

    public ArrayList<T> select(Constraint<T> constraint){
        ArrayList<T> result = new ArrayList<>();
        for (T item : data){
            if (constraint.check(item)){
                result.add(item);
            }
        }
        return result;
    }

    public void save() throws IOException {
        Gson gson = FileDBManager.getGson();
        BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(
                        new FileOutputStream(getDBPath()), StandardCharsets.UTF_8));
        writer.write(gson.toJson(this));
        writer.close();
    }
}
