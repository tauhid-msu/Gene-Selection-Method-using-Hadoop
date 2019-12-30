package gsChild4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class RedTaskFive extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
	
	private IntWritable Freq = new IntWritable();
	
	public void reduce(IntWritable key, Iterable<IntWritable> value, Context context)
					throws IOException, InterruptedException {
		
		int sum=0;
		for (IntWritable val : value) {
			sum += val.get();
		}
		Freq.set(sum);
		context.write(key,Freq);
		//context.write(Freq,key);
			
	}
}