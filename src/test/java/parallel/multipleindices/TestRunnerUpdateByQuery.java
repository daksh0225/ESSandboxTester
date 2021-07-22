package parallel.multipleindices;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.ParallelComputer;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class TestRunnerUpdateByQuery {
    static StringBuilder emp50k;
    static StringBuilder emp100k;
    private static String resource1 = "data/Employees50K.json";
    private static String resource2 = "data/Employees100K.json";

    @BeforeClass
    public static void loadContent() {
        File file1 = new File(resource1);
        emp50k = new StringBuilder();
        try(InputStream in = new FileInputStream(file1)){
            int c;
            while ((c = in.read()) != -1) {
                emp50k.append((char)c);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        File file2 = new File(resource2);
        emp100k = new StringBuilder();
        try(InputStream in = new FileInputStream(file2)){
            int c;
            while ((c = in.read()) != -1) {
                emp100k.append((char)c);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void test(){
        Class[] cls = {TestClass1.class, TestClass2.class, TestClass3.class, TestClass4.class, TestClass5.class, TestClass6.class, TestClass7.class,
        TestClass8.class, TestClass9.class, TestClass10.class};

        Result result = JUnitCore.runClasses(new ParallelComputer(false, false), cls);
        if(!result.wasSuccessful()){
            System.out.println("Tests Failed: " + result.getFailureCount());
            List<Failure> failures = result.getFailures();
            for(Failure f: failures){
                System.out.println(f.toString());
            }
        }
        assertTrue(result.wasSuccessful());
    }
}
