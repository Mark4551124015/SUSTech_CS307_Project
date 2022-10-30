import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.util.Calendar;

/**
 * All in one test script for PostgreSQL
 */
public class PostgresSQLTest {
    Loader ld = new Loader();
    DatabaseManipulation dm = new DatabaseManipulation();
    MultiThreadLoader mld = new MultiThreadLoader();

    /**
     * Test insert,update,delete,select operations together
     * @param filePath source data file
     * @param timeK factor of operation numbers
     * @throws Exception
     */
    public void TestAllOperation(String filePath, int timeK) throws Exception {
        File file = new File("log.txt");
        PrintStream ps = new PrintStream(file);
        PrintStream out = new PrintStream(System.out);
        System.out.println("Java testing：");
        TestLoader(filePath, Main.LoadSize*timeK);
        TestDelete(filePath, Main.DeleteSize*timeK);
        TestInsertion(filePath, Main.InsertSize*timeK);
        TestUpdate(filePath, Main.UpdateSize*timeK);
        TestSelect(filePath, Main.SelectSize*timeK,ps);
    }

    /**
     * Test our 6 versions of importing data together
     * @param filePath source data file
     * @throws Exception
     */
    public void TestAllLoader(String filePath) throws Exception {
        File file = new File("log.txt");
        PrintStream ps = new PrintStream(file);
        PrintStream out = new PrintStream(System.out);
        System.out.println("Java testing：");
        TestLoader_V1(filePath, 100);
        TestLoader_V2(filePath, 10000);
        TestLoader_V3(filePath, 10000);
        TestLoader_V4(filePath, 100000);
        TestLoader_V5(filePath, 100000);
        TestLoader_V6(filePath, 500000);
    }

    /**
     * Test insert operation
     * @param filePath source file path
     * @param max max number of importing data
     * @throws Exception
     */
    public void TestInsertion(String filePath, int max) throws Exception{
        dm.emptyTables();
        File csv = new File(filePath);
        csv.setReadable(true);
        csv.setWritable(true);
        InputStreamReader isr = null;
        BufferedReader br = null;
        isr = new InputStreamReader(new FileInputStream(csv), StandardCharsets.UTF_8);
        br = new BufferedReader(isr);
        String line = "";
        int cnt = 0;
        line = br.readLine();
        dm.getConnection();
        long total=0;
        while ((line = br.readLine())!= null && cnt++ < max) {
            total+=dm.addFullRecords(line);
        }
        dm.closeConnection();
        System.out.printf("Insert: %d records, speed: %.2f records/s\n",max, (float)(max*1e3/(total)));


    }

    /**
     * Test our final version of importing data
     * @param filePath
     * @param max
     * @throws Exception
     */
    public void TestLoader(String filePath, int max) throws Exception {
        dm.emptyTables();
        ld.loadFromFile(filePath,max);
    }
    /**
     * Test our first version of importing data
     * @param filePath
     * @param max
     * @throws Exception
     */
    public void TestLoader_V1(String filePath, int max) throws Exception {
        dm.emptyTables();
        ld.loadFromFile_V1(filePath,max);
    }
    /**
     * Test our second version of importing data
     * @param filePath
     * @param max
     * @throws Exception
     */
    public void TestLoader_V2(String filePath, int max) throws Exception {
        dm.emptyTables();
        ld.loadFromFile_V2(filePath,max);
    }
    /**
     * Test our third version of importing data
     * @param filePath
     * @param max
     * @throws Exception
     */
    public void TestLoader_V3(String filePath, int max) throws Exception {
        dm.emptyTables();
        ld.loadFromFile_V3(filePath,max);
    }
    /**
     * Test our forth version of importing data
     * @param filePath
     * @param max
     * @throws Exception
     */
    public void TestLoader_V4(String filePath, int max) throws Exception {
        dm.emptyTables();
        ld.loadFromFile_V4(filePath,max);
    }
    /**
     * Test our fifth version of importing data
     * @param filePath
     * @param max
     * @throws Exception
     */
    public void TestLoader_V5(String filePath, int max) throws Exception {
        dm.emptyTables();
        ld.loadFromFile_V5(filePath,max);
    }
    /**
     * Test our sixth version of importing data
     * @param filePath
     * @param max
     * @throws Exception
     */
    public void TestLoader_V6(String filePath, int max) throws Exception {
        dm.emptyTables();
        mld.loadFromFile(filePath,max);
    }

    /**
     * Test delete operation
     * @param filePath
     * @param max
     * @throws Exception
     */
    public void TestDelete(String filePath, int max) throws Exception {
        File csv = new File(filePath);
        csv.setReadable(true);
        csv.setWritable(true);
        InputStreamReader isr = null;
        BufferedReader br = null;
        isr = new InputStreamReader(new FileInputStream(csv), StandardCharsets.UTF_8);
        br = new BufferedReader(isr);
        String line = "";
        int cnt = 0;
        line = br.readLine();
        long startTime=System.currentTimeMillis();
        dm.getConnection();
        String[] Info;
        String[] Name = new String[max];
        while ((line = br.readLine())!= null && cnt < max) {
            Info = line.split(",",-1);
            Name[cnt++] = Info[0];
        }
        dm.deleteByItemName(Name);
        dm.closeConnection();
        long endTime = System.currentTimeMillis();
    }

    /**
     * Test output operation
     * @param filePath
     * @param max
     * @throws Exception
     */
    public void TestUpdate(String filePath, int max) throws Exception {
        File csv = new File(filePath);
        csv.setReadable(true);
        csv.setWritable(true);
        InputStreamReader isr = null;
        BufferedReader br = null;
        isr = new InputStreamReader(new FileInputStream(csv), StandardCharsets.UTF_8);
        br = new BufferedReader(isr);
        String line = "";
        int cnt = 0;
        line = br.readLine();
        long startTime=System.currentTimeMillis();
        dm.getConnection();
        String[] Info;
        String name;
        long total = 0;
        while ((line = br.readLine())!= null && cnt++ < max) {
            Info = line.split(",",-1);
            name = Info[5];
            total += dm.updateCourier(name+",H,"+CalBirth("2002-09-11",48)+","+cnt+100000000+",Apple,上海");
        }
        dm.closeConnection();
        long endTime = System.currentTimeMillis();
        System.out.printf("Update: %d records, speed: %.2f records/s\n",max, (float)(max*1e3/(total)));

    }

    /**
     * Test select operation
     * @param filePath
     * @param max
     * @param ps
     * @throws Exception
     */
    public void TestSelect(String filePath, int max, PrintStream ps) throws Exception {
        File file = new File("log.txt");
        PrintStream out = new PrintStream(System.out);
        System.setOut(ps);
        File csv = new File(filePath);
        csv.setReadable(true);
        csv.setWritable(true);
        InputStreamReader isr = null;
        BufferedReader br = null;
        isr = new InputStreamReader(new FileInputStream(csv), StandardCharsets.UTF_8);
        br = new BufferedReader(isr);
        String line = "";
        int cnt = 0;
        line = br.readLine();
        long startTime=System.currentTimeMillis();
        dm.getConnection();
        String[] Info;
        String name;
        long total = 0;
        while ((line = br.readLine())!= null && cnt++ < max) {
            Info = line.split(",",-1);
            name = Info[0];
            total += dm.selectShipmentByName(name);
        }
        dm.closeConnection();
        long endTime = System.currentTimeMillis();
        System.setOut(out);
        System.out.printf("Select: %d records, speed: %.2f records/s\n",max, (float)(max*1e3/(total)));

    }

    /**
     * Helper function
     * Calculate the birthday of courier
     * @param str
     * @param age
     * @return
     */
    protected static Date CalBirth (String str, float age) {
        Date date = Date.valueOf(str);
        Calendar birth = Calendar.getInstance();
        birth.setTime(date);
        birth.add(Calendar.YEAR, -1*(int)age);
        java.util.Date ret = birth.getTime();
        long temp = ret.getTime();
        Date out = new Date(temp);
        return out;
    }
}
