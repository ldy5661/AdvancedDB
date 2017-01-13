//package Top20Restaurants;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//
///**
// * Created by dongyueli on 11/21/16.
// */
//public class Runner {
//    public static void main(String[] args) throws Exception {
////        Configuration conf = new Configuration();
////        Job job = Job.getInstance(conf, "Count Users");
////        job.setJarByClass(Runner.class);
////        job.setMapperClass(ReviewMinMaxCount.ReviewMinMaxCountMapper.class);
////        job.setReducerClass(ReviewMinMaxCount.ReviewMinMaxCountReducer.class);
////        job.setOutputKeyClass(Text.class);
////        job.setOutputValueClass(ReviewMinMaxCount.ReviewMinMaxCountTuple.class);
////        FileInputFormat.addInputPath(job, new Path(args[0]));
////        FileOutputFormat.setOutputPath(job, new Path(args[1]));
////
////        System.exit(job.waitForCompletion(true) ? 0 : 1);
//
//        Configuration conf1 = new Configuration();
//        Job job1 = Job.getInstance(conf1, "Top20AZRestaurantsPrep");
//        job1.setJarByClass(Runner.class);
//        job1.setMapperClass(Top20AZRestaurantsPrep.Top20Mapper.class);
//        job1.setOutputKeyClass(NullWritable.class);
//        job1.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job1, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
//
//        job1.waitForCompletion(true);
//
//        Configuration conf2 = HBaseConfiguration.create();
//        Job job2 = Job.getInstance(conf2, "Top20Restaurants");
//        job2.setJarByClass(Runner.class);
//        job2.setMapperClass(Top20AZRestaurants.Top20Mapper.class);
//        job2.setReducerClass(Top20AZRestaurants.Top20Reducer.class);
//        job2.setNumReduceTasks(1);
//        job2.setMapOutputKeyClass(NullWritable.class);
//        job2.setMapOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job2, new Path(args[1]));
//
//        TableMapReduceUtil.initTableReducerJob("Top20AZRestaurantsTable", Top20AZRestaurants.Top20Reducer.class, job2);
//
//        System.exit(job2.waitForCompletion(true) ? 0 : 1);
//    }
//}
