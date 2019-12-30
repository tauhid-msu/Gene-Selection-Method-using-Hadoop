package gsChild4;

import java.io.*;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class MapTaskFour extends Mapper<LongWritable, Text, IntWritable,Text>{
	
	private IntWritable tKey = new IntWritable();
	private Text word = new Text();
	
	public void map(LongWritable key, Text values, Context context)
			throws IOException, InterruptedException {
		
		String line = values.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		int j=0;
		int setid=0;
		String test2 = "";
		while (tokenizer.hasMoreTokens()) {
		
			String test = tokenizer.nextToken();
			if(j==0){
				setid = Integer.parseInt(test);
			}
			else if(j==2){
				test2 = test;
			}
			j++;
		}
		
		tKey.set(setid);
		word.set(test2);
		context.write(tKey, word);
		
	}

}
