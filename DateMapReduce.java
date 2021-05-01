import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AccessLog {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperDate extends Mapper<LongWritable, Text, Text, IntWritable> {
	    IntWritable val = new IntWritable(0);

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split(" ");
	    Text hostname = new Text();
	    hostname.set(sa[8].substring(6,15));
        val.set(Integer.parseInt(sa[9]));
	    context.write(hostname, val);
        }
    }

    public static class MapperCode extends Mapper<LongWritable, Text, Text, IntWritable> {
	    IntWritable val = new IntWritable(1);

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split(" ");
	    Text hostname = new Text();
	    hostname.set(sa[8]);
	    context.write(hostname, val);
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();
    
        @Override
	protected void reduce(Text word, Iterable<IntWritable> intOne,
			      Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = intOne.iterator();
        
            while (itr.hasNext()){
                sum  += itr.next().get();
            }
            result.set(sum);
            context.write(word, result);
       }
    }

}
