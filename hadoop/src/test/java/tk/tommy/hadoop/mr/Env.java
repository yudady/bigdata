package tk.tommy.hadoop.mr;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class Env {

    static {
        Properties properties = System.getProperties();
        Iterator<Map.Entry<String, String>> iterator = System.getenv().entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> pair = iterator.next();
            String key = pair.getKey();
            String value = pair.getValue();
            System.out.println(key + " " + value);
            System.setProperty(key, value);
        }
    }
}
