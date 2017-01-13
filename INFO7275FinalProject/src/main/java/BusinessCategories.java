import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

/**
 * Created by dongyueli on 11/21/16.
 */
public class BusinessCategories {
    public static class CategoriesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context)
                throws IOException,InterruptedException {
            String line = value.toString();
            JSONObject jsonObj = null;
            try {
                jsonObj = new JSONObject(line);
                if(jsonObj.getString("type").equals("business")) {
                    JSONArray cateArray = jsonObj.getJSONArray("categories");
                    for (int i = 0; i < cateArray.length(); i++) {
                        context.write(new Text(cateArray.get(i).toString()), one);
                    }
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

    }

    public static class CategoriesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int counter = 0;

            for(IntWritable val : values) {
                counter += val.get();
            }
            context.write(key, new IntWritable(counter));
        }
    }
}
