
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author dongyueli
 */
public class CategoriesInvertedIndex {
    public static class CategoriesMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException,InterruptedException {
            String line = value.toString();
            JSONObject jsonObj = null;
            try {
                jsonObj = new JSONObject(line);
                if(jsonObj.getString("type").equals("business")) {
                    JSONArray cateArray = jsonObj.getJSONArray("categories");
                    for (int i = 0; i < cateArray.length(); i++) {
                        context.write(new Text(cateArray.get(i).toString()), new Text(jsonObj.getString("business_id")));
                    }
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

    }
    
    public static class CategoriesReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            boolean first = true;

            for(Text val : values) {
                if (first) {
                    first = false;
                } else {
                    sb.append(" ");
                }
                sb.append(val.toString());
            }
            
            result.set(sb.toString());
            context.write(key, result);
            
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(CategoriesInvertedIndex.class);
        job.setMapperClass(CategoriesMapper.class);
        job.setReducerClass(CategoriesReducer.class);
        job.setCombinerClass(CategoriesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
