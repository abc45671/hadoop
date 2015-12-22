package Titanic.pro1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Administrator on 2015/12/22.
 */
public class reduce {
    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double avg_age = 0;
            double sum = 0;
            int count = 0;
            for (DoubleWritable v : values) {
                sum = sum + v.get();
                count++;
            }
            avg_age = sum/count;
            context.write(key, new DoubleWritable(avg_age));
        }
    }
}
