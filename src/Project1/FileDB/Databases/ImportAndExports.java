package Project1.FileDB.Databases;

import Project1.FileDB.Databases.Models.ImportAndExport;

import java.util.ArrayList;

public class ImportAndExports extends BaseModel<ImportAndExport> {
    protected int selfIncreasingNumber;

    @Override
    public void initialize() {
        this.data = new ArrayList<>();
    }

    @Override
    public String getDBPath() {
        return "./FileDatabase/import-and-export.json";
    }
}
