package gsChild4;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.lang.StringUtils;

public class RedTaskTwo extends Reducer<LongWritable,Text,LongWritable,Text> {
	
	private Text word = new Text();
	private LongWritable tkey = new LongWritable();
	
	boolean first = true;
	StringBuilder toReturn = new StringBuilder();
    
	public void reduce(LongWritable key, Iterable<Text> value, Context context)
					throws IOException, InterruptedException {
		
		int i,trNum,tstNum,Tj;
		int lkey;
		double dist=0.0;
		String[] line;
		
		Configuration conf=context.getConfiguration();
		String smstr = conf.get("smpls"); 
		String[] smp_cls = StringUtils.split(smstr,"=");
		
		List<Integer> pSetList = new ArrayList<Integer>();
		
		for (Text val : value) {
			
			line = StringUtils.split(val.toString(),' ');
			
			dist += Integer.parseInt(line[0]);
			/*
			if(!pSetList.contains(Integer.parseInt(line[1]) )){
				pSetList.add( Integer.parseInt(line[1]) );
			}
			*/
			if(!first){toReturn.append(",");}
			first = false;
			toReturn.append(line[1]);
			//toReturn.append(val.toString());
				
		}
		//String pSetF = implodeArray(convertIntegers(pSetList),",");
		word.set(String.valueOf(dist)+" "+toReturn.toString());
		//word.set(String.valueOf(dist)+" "+pSetF);
		context.write(key, word);
		
		toReturn.setLength(0);
		first = true;			
	}
	
	
	public static int[] convertIntegers(List<Integer> integers)	{
	    int[] ret = new int[integers.size()];
	    Iterator<Integer> iterator = integers.iterator();
	    for (int i = 0; i < ret.length; i++)
	    {
	        ret[i] = iterator.next().intValue();
	    }
	    return ret;
	}
	
	public static String implodeArray(int[] inputArray, String glueString) {

		String output = "";

		if (inputArray.length > 0) {
			StringBuilder sb = new StringBuilder();
			sb.append(inputArray[0]);

			for (int i=1; i<inputArray.length; i++) {
				sb.append(glueString);
				sb.append(inputArray[i]);
			}

			output = sb.toString();
		}
		return output;

	}
			
}
