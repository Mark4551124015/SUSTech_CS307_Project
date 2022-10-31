public class Main {
    static final int LoadSize = 10000;
    static final int DeleteSize = 10000;
    static final int InsertSize = 10000;
    static final int UpdateSize = 10000;
    static final int SelectSize = 10000;
    static final String filePath = "data/shipment_records.csv";
    public static void main(String[] args) throws Exception {
        PostgresSQLTest jt = new PostgresSQLTest();
        DatabaseManipulation dm = new DatabaseManipulation();
//        dm.QueryBestPort("pear","export");
//        dm.QueryBestPort("apple","import");
//        dm.QueryServedContainer(540);
//        dm.QueryServedContainer(511);

//        dm.QueryBestCourier("北京","拼多多",5);
//        dm.QueryBestCourier("重庆","唯品会",5);
        jt.dm.emptyTables();

        jt.ld.loadFromFile(filePath,1000);




    }

}