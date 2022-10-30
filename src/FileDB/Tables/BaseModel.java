package FileDB.Tables;

import FileDB.Tables.Models.Constraint;
import FileDB.FileDBManager;
import com.google.gson.Gson;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public abstract class BaseModel<T> {
    public ArrayList<T> data;

    public abstract void initialize();

    public File getDBFile() {
        return new File(getDBPath());
    }

    public abstract String getDBPath();

    public ArrayList<T> select(Constraint<T> constraint) {
        ArrayList<T> result = new ArrayList<>();
        for (T item : data) {
            if (constraint.check(item)) {
                result.add(item);
            }
        }
        return result;
    }

    public void save() {
        Gson gson = FileDBManager.getGson();

        try {
            BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(
                            new FileOutputStream(getDBPath()), StandardCharsets.UTF_8));
            writer.write(gson.toJson(this));
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int delete(Constraint<T> constraint) {
        int affectedLines = 0;

        Iterator<T> iterator = data.iterator();
        while (iterator.hasNext()) {
            T item = iterator.next();
            if (constraint.check(item)) {
                iterator.remove();
                affectedLines++;
            }
        }

        return affectedLines;
    }

    public void shuffle() {
        Collections.shuffle(this.data);
    }
}
