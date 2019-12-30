package gsChild4;

import java.io.*;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class MapTaskFive extends Mapper<LongWritable, Text, IntWritable,IntWritable>{
	
	private IntWritable Gi = new IntWritable();
	private IntWritable One = new IntWritable(1);
	
	public void map(LongWritable key, Text values, Context context)
			throws IOException, InterruptedException {
		
		String line = values.toString();
		//String[] pSet = new String[]{};
		int i,j;
		
		StringTokenizer tokenizer = new StringTokenizer(line);
		j=0;
		while (tokenizer.hasMoreTokens()) {
		
			String test = tokenizer.nextToken();
			if(j==2){
				String[] pSet = StringUtils.split(test,',');
				for(i=0; i<pSet.length;i++){
					Gi.set(Integer.parseInt(pSet[i]));
					context.write(Gi,One);
				}
			}
			j++;
	
		}
		
	}

}
