package parallel.multipleindices;

import com.example.estester.Application;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static parallel.multipleindices.TestRunnerUpdateByQuery.emp100k;
import static parallel.multipleindices.TestRunnerUpdateByQuery.emp50k;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestClass9 {
    private static StringBuilder content1;
    private static StringBuilder content2;
    private static String resource1 = "data/Employees50K.json";
    private static String resource2 = "data/Employees100K.json";
    private static String sandboxId = null;

//    @BeforeClass
    public static void loadContent() {
        File file1 = new File(resource1);
        content1 = new StringBuilder();
        try(InputStream in = new FileInputStream(file1)){
            int c;
            while ((c = in.read()) != -1) {
                content1.append((char)c);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        File file2 = new File(resource2);
        content2 = new StringBuilder();
        try(InputStream in = new FileInputStream(file2)){
            int c;
            while ((c = in.read()) != -1) {
                content2.append((char)c);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Files read successfully");
    }

    @BeforeClass
    public static void getSandbox(){
        try {
            URL url = new URL("http://localhost:9200/_sandbox/get");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            con.connect();

            int code = con.getResponseCode();
            if(code == 200){
                System.out.println("Sandbox acquired");
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuffer content = new StringBuffer();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                in.close();
                JSONObject jsonObject = new JSONObject(content.toString());
                String sb = jsonObject.getString("sandboxId").toString();
                sandboxId = sb;
                System.out.println(sandboxId);
            }
            else
            {
                System.out.println("Error requesting sandbox");
            }
            con.disconnect();
        }
        catch(Exception e){
            System.err.println(e.getMessage());
        }
    }

    @AfterClass
    public static void setup(){
        try {
            URL url = new URL("http://localhost:9200/employees*");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("DELETE");
            con.setRequestProperty("Sandbox", sandboxId);
            con.connect();

            int code = con.getResponseCode();
            if(code == 200){
                System.out.println("Index deleted successfully");
            }
            else{
                System.out.println("Index doesn't exist");
            }
            con.disconnect();
        }
        catch(Exception e){
            System.err.println(e.getMessage());
        }
    }

    @Test
    public void testA(){
        int code = Application.bulkRequest(6, emp50k.toString(), sandboxId, "employees50k");
//        System.out.println(code);
        assertEquals(200, code);
    }

    @Test
    public void testB(){
        int code = Application.bulkRequest(6, emp100k.toString(), sandboxId, "employees100k");
//        System.out.println(code);
        assertEquals(200, code);
    }

    @Test
    public void testC() throws InterruptedException {
        Thread.sleep(1000);
        String index = "employees50k,employees100k";
        String queryPayload = "{\n  \"track_total_hits\": true, \"query\":\n  {\n    \"match\": {\"MaritalStatus\": \"Married\"}\n  }\n}";
        int val = Application.searchIndex(index, queryPayload, sandboxId);
//        System.out.println("1: " + val);
        assertEquals(75054, val);
    }

    @Test
    public void testD() throws InterruptedException {
        Thread.sleep(1000);
        String index = "employees50k,employees100k";
        String queryPayload =
        "{" +
                "\"query\":{" +
                "\"bool\":{" +
                "\"should\": ["+
                "{\"match\": {\"Gender\": \"Male\"}}," +
                "{\"range\": {\"Age\": {\"gte\": 40}}}" +
                "]}}," +
                "\"script\":{" +
                "\"source\": \"ctx._source.MaritalStatus = params.state\", \"params\": {\"state\": \"Unmarried\"}, \"lang\": \"painless\"" +
                "}" +
        "}";
        int code = Application.updateByQueryRequest(6, index, queryPayload, sandboxId);
//        System.out.println("1: " + code);
        assertEquals(200, code);
    }

    @Test
    public void testE() throws InterruptedException {
        Thread.sleep(1000);
        String index = "employees50k,employees100k";
        String queryPayload = "{\n  \"track_total_hits\": true, \"query\":\n  {\n    \"match\": {\"MaritalStatus\": \"Married\"}\n  }\n}";
        int val = Application.searchIndex(index, queryPayload, sandboxId);
//        System.out.println("1: " + val);
        assertEquals(36084, val);
    }
}
