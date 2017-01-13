//package SecondarySortBusiness;
//
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.DoubleWritable;
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
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "Categories Count Star Average");
//        job.setJarByClass(Runner.class);
//        job.setMapperClass(CategoriesCountAndStarAvg.CategoriesMapper.class);
//        job.setReducerClass(CategoriesCountAndStarAvg.CategoriesReducer.class);
//        job.setCombinerClass(CategoriesCountAndStarAvg.CategoriesReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(CategoriesCountAndStarAvg.CountAvgStarsTuple.class);
//
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//        job.waitForCompletion(true);
//
//        Configuration conf2 = new Configuration();
//        Job job2 = Job.getInstance(conf2, "SS Categories Count Star Average");
//        job2.setJarByClass(Runner.class);
//        job2.setMapperClass(SecondarySort.SsMapper.class);
//        job2.setReducerClass(SecondarySort.SsReducer.class);
//        job2.setPartitionerClass(NaturalKeyPartitioner.class);
//        job2.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
//        job2.setSortComparatorClass(CompositeSortComparator.class);
//        job2.setMapOutputKeyClass(CompositeKeyWritable.class);
//        job2.setMapOutputValueClass(Text.class);
//        job2.setNumReduceTasks(9);
//
//        FileInputFormat.addInputPath(job2, new Path(args[1]));
//        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
//
//        System.exit(job2.waitForCompletion(true) ? 0 : 1);
//
//    }
//}
