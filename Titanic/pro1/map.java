package Titanic.pro1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Administrator on 2015/12/22.
 */
public class map {
    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text k = new Text();
        private DoubleWritable val = new DoubleWritable();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] s = line.split(",");
            int survied = Integer.parseInt(s[1]);
            if (s.length > 1) {
                if(survied == 1){
                    k.set(s[4]);
                    if(s[5].length() >= 1) {
                        val.set(Double.parseDouble(s[5]));
                        context.write(k, val);
                    }
                }
            }
        }
    }
}
