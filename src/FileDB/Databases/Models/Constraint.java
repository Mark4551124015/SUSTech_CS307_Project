package FileDB.Databases.Models;

public interface Constraint<T> {
    boolean check(T model);

}
