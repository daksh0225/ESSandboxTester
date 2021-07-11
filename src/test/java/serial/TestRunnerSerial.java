package serial;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.ParallelComputer;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.RunWith;
import org.junit.runner.notification.Failure;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRunnerSerial {

    @Test
    public void test(){
        Class[] cls = {TestClass1.class, TestClass2.class, TestClass3.class, TestClass4.class, TestClass5.class};

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
