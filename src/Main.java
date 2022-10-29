import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

public class Main {
    static final int LoadSize = 100000;
    static final int DeleteSize = 100;
    static final int InsertSize = 100;
    static final int UpdateSize = 100;
    static final int SelectSize = 100;
    static final String filePath = "data/shipment_records.csv";

    public static void main(String[] args) throws Exception {
        JavaTests jt = new JavaTests();


        //second atgs of JavaTests is the constant which timed the parameters up there
        jt.TestLoader_V4(filePath, LoadSize);




    }

}