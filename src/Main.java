import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

public class Main {
    static final int LoadSize = 10000;
    static final int DeleteSize = 1000;
    static final int InsertSize = 1000;
    static final int UpdateSize = 1000;
    static final int SelectSize = 6000;
    static final String filePath = "data/shipment_records.csv";

    public static void main(String[] args) throws Exception {
        JavaTests jt = new JavaTests();


        //second atgs of JavaTests is the constant which timed the parameters up there
//        jt.TestAllLoader(filePath,1);
        jt.ld.loadFromFile(filePath,100000);
//        jt.dm.emptyTables();




    }

}