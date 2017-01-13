package DistinctUser;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by dongyueli on 11/21/16.
 */
public class CountUsersReducer extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
    public void reduce(NullWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int counter = 0;

        for(IntWritable val : values) {
            counter += val.get();
        }

        context.write(NullWritable.get(), new IntWritable(counter));
    }

}
