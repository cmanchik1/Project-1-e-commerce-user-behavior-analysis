import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PurchaseTimeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text hourCategoryKey = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        String timestamp = fields[6];  // Corrected index
        String category = fields[2];
        int hour = Integer.parseInt(timestamp.split(" ")[1].split(":")[0]);
        hourCategoryKey.set(hour + "_" + category);
        context.write(hourCategoryKey, one);
    }
}
