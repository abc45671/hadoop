package Titanic.pro2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Administrator on 2015/12/23.
 */
public class reduce {
    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double avg_fare = 0;
            double sum = 0;
            int count = 0;
            for (DoubleWritable v : values) {
                sum = sum + v.get();
                count++;
            }
            avg_fare = sum/count;
            context.write(key, new DoubleWritable(avg_fare));
        }
    }
}
