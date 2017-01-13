//package JoinBusinessTip;
//
//import Top20Restaurants.Top20AZRestaurants;
//import Top20Restaurants.Top20AZRestaurantsPrep;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//
///**
// * Created by dongyueli on 11/21/16.
// */
//public class Runner {
//    public static void main(String[] args) throws Exception {
//
//        Configuration conf = HBaseConfiguration.create();
//        Job job = Job.getInstance(conf, "BusinessTipReduceSideJoin");
//        job.setJarByClass(Runner.class);
//        job.setReducerClass(BusinessTipReduceSideJoin.BusinessTipReducer.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//
//        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, BusinessTipReduceSideJoin.BusinessMapper.class);
//        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, BusinessTipReduceSideJoin.TipMapper.class);
//        TableMapReduceUtil.initTableReducerJob("BusinessTipReduceSideJoinTable", BusinessTipReduceSideJoin.BusinessTipReducer.class, job);
//        job.getConfiguration().set("join.type", args[2]);
//
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//
//    }
//}
