package Titanic.pro5;

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
            if (s.length >= 5) {
                int survied = Integer.parseInt(s[1]);
                if(survied == 1){
                    String died_PeopleClassSex = s[2] + "\t" + s[4];
                    k.set(died_PeopleClassSex);
                    val.set(1);
                    context.write(k, val);
                }
            }
        }
    }
}
