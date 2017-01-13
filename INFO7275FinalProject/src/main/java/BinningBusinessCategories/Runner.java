//package BinningBusinessCategories;
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
//import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//
///**
// * Created by dongyueli on 11/21/16.
// */
//public class Runner {
//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "Binning Bussiness Categories");
//        job.setJarByClass(Runner.class);
//        job.setMapperClass(BusinessCategoriesBinning.BinningMapper.class);
//        job.setNumReduceTasks(0);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(NullWritable.class);
//
//        MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class, Text.class, NullWritable.class);
//        MultipleOutputs.setCountersEnabled(job, true);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//
//    }
//}
