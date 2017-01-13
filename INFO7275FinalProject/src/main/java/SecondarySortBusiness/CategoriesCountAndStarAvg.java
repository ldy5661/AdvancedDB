package SecondarySortBusiness;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by dongyueli on 12/11/16.
 */
public class CategoriesCountAndStarAvg {
    public static class CategoriesMapper extends Mapper<LongWritable, Text, Text, CountAvgStarsTuple> {
        private CountAvgStarsTuple outCountStar = new CountAvgStarsTuple();

        public void map(LongWritable key, Text value, Context context)
                throws IOException,InterruptedException {
            String line = value.toString();
            JSONObject jsonObj = null;
            try {
                jsonObj = new JSONObject(line);
                if(jsonObj.getString("type").equals("business")) {
                    JSONArray cateArray = jsonObj.getJSONArray("categories");
                    for (int i = 0; i < cateArray.length(); i++) {
                        outCountStar.setCount(1);
                        outCountStar.setStars(jsonObj.getDouble("stars"));
                        context.write(new Text(cateArray.get(i).toString()), outCountStar);
                    }
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

    }

    public static class CategoriesReducer extends Reducer<Text, CountAvgStarsTuple, Text, CountAvgStarsTuple> {

        private CountAvgStarsTuple result = new CountAvgStarsTuple();

        public void reduce(Text key, Iterable<CountAvgStarsTuple> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;

            for(CountAvgStarsTuple val : values) {
                sum += val.getStars() * val.getCount();
                count += val.getCount();
            }

            double avg = Math.round((sum / count) * 10) / 10.0;

            result.setCount(count);
            result.setStars(avg);

            context.write(key, result);
        }
    }


    public static class CountAvgStarsTuple implements Writable {
        private double stars = 0.0;
        private long count = 0;

        public double getStars() {
            return stars;
        }

        public void setStars(double stars) {
            this.stars = stars;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public void readFields(DataInput in) throws IOException {
            stars = in.readDouble();
            count = in.readLong();
        }

        public void write(DataOutput out) throws IOException {
            out.writeDouble(stars);
            out.writeLong(count);
        }

        public String toString(){
            return stars + "\t" + count;
        }
    }



}
