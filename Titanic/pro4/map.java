package Titanic.pro4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Administrator on 2015/12/22.
 */
public class map {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text k = new Text();
        private IntWritable val = new IntWritable();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] s = line.split(",");
            if (s.length >= 6) {
                int survied = Integer.parseInt(s[1]);
                String people = survied + "\t" + s[4] + "\t" + s[5];
                k.set(people);
                val.set(1);
                context.write(k, val);
            }
        }
    }
}
