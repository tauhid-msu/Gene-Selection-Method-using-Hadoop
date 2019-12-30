package gsChild4;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.lang.StringUtils;

public class RedTaskFour extends Reducer<IntWritable,Text,IntWritable,Text> {
	
	private Text word = new Text();
	
	public void reduce(IntWritable key, Iterable<Text> value, Context context)
					throws IOException, InterruptedException {
		
		int tstNum;
		double fs=0.0;
		String[] line;
		String pSetF = "";
		
		Configuration conf=context.getConfiguration();
		tstNum = conf.getInt("tsNum", 42);
				
		for (Text val : value) {
			
			if(fs<0.5){
				//line = StringUtils.split(val.toString(),' ');
				pSetF = val.toString();
			}
			fs +=1;
				
		}
		fs = fs / tstNum;
		
		//output Predictor sets
		if(fs>=0.88){
			word.set(String.valueOf(fs)+" "+pSetF);
			context.write(key,word);
		}
		
	}
}