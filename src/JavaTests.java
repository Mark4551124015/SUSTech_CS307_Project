import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Statement;
import java.util.Calendar;

public class JavaTests {
    Loader ld = new Loader();
    DatabaseManipulation dm = new DatabaseManipulation();
    public JavaTests(){
    }
    public void TestAllOperation(String filePath, int timeK) throws Exception {
        File file = new File("log.txt");
        PrintStream ps = new PrintStream(file);
        PrintStream out = new PrintStream(System.out);
        System.out.println("Java testing：");
        TestInsertion(filePath, Main.InsertSize*timeK);
        dm.emptyTables();
        TestLoader(filePath, Main.LoadSize*timeK);
        TestUpdate(filePath, Main.UpdateSize*timeK);
        TestSelect(filePath, Main.SelectSize*timeK,ps);
        TestDelete(filePath, Main.DeleteSize*timeK);
    }
    public void TestAllLoader(String filePath, int timeK) throws Exception {
        File file = new File("log.txt");
        PrintStream ps = new PrintStream(file);
        PrintStream out = new PrintStream(System.out);
        System.out.println("Java testing：");
//        TestLoader_V1(filePath, 100*timeK);
//        TestLoader_V2(filePath, 10000*timeK);
//        TestLoader_V3(filePath, 10000*timeK);
        TestLoader_V4(filePath, 100000*timeK);
//        TestLoader_V5(filePath, 100000*timeK);
    }

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
    public void TestLoader(String filePath, int max) throws Exception {
        dm.emptyTables();
        ld.loadFromFile_V4(filePath,max);
    }

    public void TestLoader_V1(String filePath, int max) throws Exception {
        dm.emptyTables();
        ld.loadFromFile_V1(filePath,max);
    }
    public void TestLoader_V2(String filePath, int max) throws Exception {
        dm.emptyTables();
        ld.loadFromFile_V2(filePath,max);
    }
    public void TestLoader_V3(String filePath, int max) throws Exception {
        dm.emptyTables();
        ld.loadFromFile_V3(filePath,max);
    }
    public void TestLoader_V4(String filePath, int max) throws Exception {
        dm.emptyTables();
        ld.loadFromFile_V4(filePath,max);
    }
    public void TestLoader_V5(String filePath, int max) throws Exception {
        dm.emptyTables();
        ld.loadFromFile_V5(filePath,max);
    }
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
    public static Date CalBirth (String str, float age) {
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
