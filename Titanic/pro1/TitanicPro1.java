package Titanic.pro1;

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

import java.io.IOException;

/**
 * Created by Administrator on 2015/12/22.
 * Problem statement 1:
 * In this problem statement we will find the average age of males and females who died in the Titanic tragedy.
 */
public class TitanicPro1 {
    public static void c1() throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "titanic_pro1");
        job.setJarByClass(TitanicPro1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapperClass(map.Map.class);
        job.setReducerClass(reduce.Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        String input = "/user/hadoop/titanic/TitanicData";
        FileSystem hdfs = FileSystem.get(conf);
        Path findf = new Path(input);
        boolean isExists = hdfs.exists(findf);
        if (isExists == true) {
            FileInputFormat.addInputPath(job, new Path(input));
        }
        String output = "/user/hadoop/titanic/pro1_out";
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(output), true);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
    public static void main(String args[]) throws Exception {
        c1();
    }
}
