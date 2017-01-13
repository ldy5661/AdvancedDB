package SecondarySortBusiness;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by dongyueli on 12/11/16.
 */
public class SecondarySort {
    public static class SsMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            String categoryName = parts[0];
            double stars = Double.parseDouble(parts[1]);
            int count = Integer.parseInt(parts[2]);

            CompositeKeyWritable out = new CompositeKeyWritable(stars, count);
            context.write(out, new Text(categoryName));
        }
    }

    public static class SsReducer extends Reducer<CompositeKeyWritable, Text, CompositeKeyWritable, Text> {

        public void reduce(CompositeKeyWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {
                context.write(key, val);
            }
        }
    }
}

