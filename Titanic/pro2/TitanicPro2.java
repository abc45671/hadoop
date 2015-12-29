package Titanic.pro2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by Administrator on 2015/12/23.
 * Problem statement 2:
 * In this problem statement we will find the average fare of each class.
 */
public class TitanicPro2 {
    public static void c1() throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "titanic_pro2");
        job.setJarByClass(TitanicPro2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapperClass(Titanic.pro2.map.Map.class);
        job.setReducerClass(Titanic.pro2.reduce.Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        String input = "/user/hadoop/titanic/TitanicData";
        FileSystem hdfs = FileSystem.get(conf);
        Path findf = new Path(input);
        boolean isExists = hdfs.exists(findf);
        if (isExists == true) {
            FileInputFormat.addInputPath(job, new Path(input));
        }
        String output = "/user/hadoop/titanic/pro2_out";
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(output), true);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
    public static void main(String args[]) throws Exception {
        c1();
    }
}
