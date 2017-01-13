package Top20Restaurants;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by dongyueli on 12/10/16.
 */
public class Top20AZRestaurantsPrep {
    public static class Top20Mapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private Text outValue = new Text();

        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            JSONObject jsonObj = null;
            try {
                jsonObj = new JSONObject(line);
                if (jsonObj.getString("type").equals("business") && jsonObj.getString("state").equals("AZ")) {
                    JSONArray cateArray = jsonObj.getJSONArray("categories");
                    ArrayList<String> categories = new ArrayList<String>();
                    for (int i = 0; i < cateArray.length(); i++) {
                        categories.add(cateArray.get(i).toString());
                    }
                    if(categories.contains("Restaurants")) {
                        String businessName = jsonObj.getString("name");
                        String business_id = jsonObj.getString("business_id");
                        String address = jsonObj.getString("full_address").replace("\n", "");
                        double stars = jsonObj.getDouble("stars");
                        outValue.set(business_id+ "%%%" + businessName + "%%%" + address + "%%%" + String.valueOf(stars));
                        context.write(NullWritable.get(), outValue);
                    }
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }
}
