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



        DatabaseManipulation dm = new DatabaseManipulation();
        dm.QueryServedContainer(540);
        dm.QueryServedContainer(511);



//        dm.QueryBestPort("pear","export");
//        dm.QueryBestPort("apple","import");
//        dm.QueryBestCourier("北京","拼多多",5);
//        dm.QueryBestCourier("重庆","唯品会",5);


//        jt.ld.loadFromFile(filePath,1000);
//        jt.dm.emptyTables();




    }

}