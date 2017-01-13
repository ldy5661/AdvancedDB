package BinningBusinessCategories;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

/**
 * Created by dongyueli on 12/10/16.
 */
public class BusinessCategoriesBinning {
    public static class BinningMapper extends Mapper<Object, Text, Text, NullWritable> {
        private MultipleOutputs<Text, NullWritable> mos = null;

        protected void setup(Context context) {
            mos = new MultipleOutputs<Text, NullWritable>(context);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            JSONObject jsonObj = null;
            try {
                jsonObj = new JSONObject(line);
                if(jsonObj.getString("type").equals("business")) {
                    JSONArray cateArray = jsonObj.getJSONArray("categories");
                    for (int i = 0; i < cateArray.length(); i++) {
                        if(cateArray.get(i).toString().equalsIgnoreCase("Restaurants")) {
                            mos.write("bins", value, NullWritable.get(), "Restaurants");
                        }
                        if(cateArray.get(i).toString().equalsIgnoreCase("Shopping")) {
                            mos.write("bins", value, NullWritable.get(), "Shopping");
                        }
                        if(cateArray.get(i).toString().equalsIgnoreCase("Food")) {
                            mos.write("bins", value, NullWritable.get(), "Food");
                        }
                        if(cateArray.get(i).toString().equalsIgnoreCase("Nightlife")) {
                            mos.write("bins", value, NullWritable.get(), "Nightlife");
                        }
                    }

                    if(jsonObj.getString("state").equals("WI")) {
                        mos.write("bins", value, NullWritable.get(), "State_AZ");
                    }
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }
}
