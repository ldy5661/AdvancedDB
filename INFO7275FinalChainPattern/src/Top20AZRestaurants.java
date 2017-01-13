
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
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
public class Top20AZRestaurants extends Configured implements Tool{
    public static class Top20Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            JSONObject jsonObj = null;
            try {
                jsonObj = new JSONObject(line);
                if (jsonObj.getString("type").equals("business") && jsonObj.getString("state").equals("AZ")) {
                    JSONArray cateArray = jsonObj.getJSONArray("categories");
                    ArrayList<String> categories = new ArrayList<>();
                    for (int i = 0; i < cateArray.length(); i++) {
                        categories.add(cateArray.get(i).toString());
                    }
                    if(categories.contains("Restaurants")) {
                        String businessName = jsonObj.getString("name");
                        String business_id = jsonObj.getString("business_id");
                        String address = jsonObj.getString("full_address").replace("\n", "");
                        double stars = jsonObj.getDouble("stars");
                        outValue.set(businessName + "%%%" + address + "%%%" + String.valueOf(stars));
                        context.write(new Text(business_id), outValue);
                    }
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }
    
    public static class Top20MapperRedo extends Mapper<Text, Text, DoubleWritable, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("%%%");
            String name = splits[0];
            String address = splits[1];
            double instar = Double.parseDouble(splits[2]);
            context.write(new DoubleWritable(instar), new Text(key + "%%%" + name + "%%%" + address));
        }
    }
    
    public static class Top20ReducerRedo extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
        static int counter = 0;

        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                if (counter < 20) {
                    String[] out = val.toString().split("%%%");
                    String outKey = out[0];
                    String outName = out[1];
                    String outAddress = out[2];  
                    context.write(new Text(outKey + "\t" + outName + "\t" + outAddress), key);
                    counter++;
                }
            }
        }
    }
    
    public static class MyDecreasingDoubleComparator extends WritableComparator {
        public MyDecreasingDoubleComparator() {
            super(DoubleWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            double thisValue = readDouble(b1, s1);
            double thatValue = readDouble(b2, s2);
            return (-1) * (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
        }
    }
    
    
    @Override
    public int run(String[] args) throws Exception {       
        Job job1 = new Job(getConf());
        job1.setSortComparatorClass(MyDecreasingDoubleComparator.class);
        job1.setJobName("Top20 AZ Restaurants ChainJob");
        job1.setJarByClass(Top20AZRestaurants.class);

        JobConf map1Conf = new JobConf(false);
        ChainMapper.addMapper(job1, 
                              Top20Mapper.class, 
                              LongWritable.class, 
                              Text.class, 
                              Text.class, 
                              Text.class, 
                              map1Conf);      
        JobConf map2Conf = new JobConf(false);
        ChainMapper.addMapper(job1, 
                              Top20MapperRedo.class, 
                              Text.class, 
                              Text.class, 
                              DoubleWritable.class, 
                              Text.class, 
                              map2Conf);
        JobConf reduceConf = new JobConf(false);
        ChainReducer.setReducer(job1, 
                                Top20ReducerRedo.class, 
                                DoubleWritable.class,
                                Text.class, 
                                Text.class, 
                                DoubleWritable.class, 
                                reduceConf);
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        boolean success = job1.waitForCompletion(true);
        return success ? 0 : 1;
    }
    
    public static void main(String[] args) throws Exception {
            int exitCode = ToolRunner.run(new Configuration(),
                            new Top20AZRestaurants(), args);
            System.exit(exitCode);
    }
    
}
