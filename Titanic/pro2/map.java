package Titanic.pro2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Administrator on 2015/12/23.
 */
public class map {
    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text k = new Text();
        private DoubleWritable val = new DoubleWritable();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] s = line.split(",");
            if (s.length >= 10) {
                double fare; //票價
                k.set(s[2]);
                if(s[9].length() >= 1) {
                    fare = Double.parseDouble(s[9]);
                    val.set(fare);
                    context.write(k, val);
                }
            }
        }
    }
}
