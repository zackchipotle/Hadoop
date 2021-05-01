package Lab3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> <input dir> <output dir>");
		System.out.println("Job class is either RequestCount or ByteCount");
	    System.exit(-1);
	} else if ("RequestCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(CodeMapReducer.ReducerImpl.class);
	    job.setMapperClass(CodeMapReducer.MapperImpl.class);
	    job.setOutputKeyClass(CodeMapReducer.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(CodeMapReducer.OUTPUT_VALUE_CLASS);
	} else if ("ByteCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(DateMapReducer.ReducerImpl.class);
	    job.setMapperClass(DateMapReducer.MapperImpl.class);
	    job.setOutputKeyClass(DateMapReducer.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(DateMapReducer.OUTPUT_VALUE_CLASS);
	} else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}