package DistinctUser;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

/**
 * Created by dongyueli on 11/21/16.
 */
public class CountUsersMapper extends Mapper<LongWritable, Text, NullWritable, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context)
        throws IOException,InterruptedException {
        String line = value.toString();
        JSONObject jsonObj = null;
        try {
            jsonObj = new JSONObject(line);
            if(jsonObj.getString("type").equals("user")) {
                context.write(NullWritable.get(), one);
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

}
