package parallel.singledocument;

import com.example.estester.Application;
import org.json.JSONObject;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestClass1 {
    private static String sandboxId;

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

    @BeforeClass
    @AfterClass
    public static void setup(){
        try {
            URL url = new URL("http://localhost:9200/daksh0225");
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
    public void testDocumentPostRequest(){
        String url = "http://localhost:9200/daksh0225/_doc?refresh";
        String payload = "{\"type\": \"table\", \"company\": \"woodtech1\"}";
        int code = Application.putDocument(1, url, payload, "POST", this.sandboxId);
        System.out.println("1: " + code);
        assertEquals(201, code);
    }

    @Test
    public void testSearching() throws InterruptedException {
        Thread.sleep(1000);
        String payload = "{\"type\": \"table\", \"company\": \"woodtech1\"}";
        String index = "daksh0225";
        JSONObject doc = new JSONObject(payload);
        String queryPayload = "{\n  \"query\":\n  {\n    \"match\": {\"type\": \"" + doc.getString("type") + "\"}\n  }\n}";
        int val = Application.searchIndex(index, queryPayload, this.sandboxId);
        System.out.println("1: " + val);
        assertEquals(1, val);
    }
}
