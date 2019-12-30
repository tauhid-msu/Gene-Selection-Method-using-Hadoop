package gsChild4;

import java.io.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class MapTaskTwo extends Mapper<LongWritable, Text, LongWritable,Text>{
	
	private Text word = new Text();
	private LongWritable m2keyOut = new LongWritable();
	
	public void map(LongWritable key, Text values, Context context)
			throws IOException, InterruptedException {
		String line = values.toString();
		
		double Tij=0.0,TRij=0.0;
		int dist=0;
		int i=0,j,gid=0;
		long m2key=0;
		String[] SLl = new String[]{};
		
		Configuration conf =context.getConfiguration();
		
		//updated code from this point
		StringTokenizer tokenizer = new StringTokenizer(line);
		j=0;
		while (tokenizer.hasMoreTokens()) {
		
			String test = tokenizer.nextToken();
			if(j==0){
				m2key = Long.parseLong(test);
			}
			else if(j==1){
				Tij = Double.parseDouble(test);
			}
			else if(j==2){
				TRij = Double.parseDouble(test);
			}
			else if(j==3){
				SLl = StringUtils.split(conf.get(test),",");
			}
			j++;
		}
		//Map input ends
		
		//Distance Measure function
		dist = (int)Math.pow( (Tij - TRij), 2);
		
		//remove gid from key 
		gid = (int)(m2key % 10000);
		m2key = m2key / 10000;
		
		//Map2 <key,value> emit starts
		for(i=0; i<SLl.length;i++){
			if(m2key>0){
				long m2keyNew = (Integer.parseInt(SLl[i])*1000000)+m2key;
				word.set(new byte[0]);
				m2keyOut.set(m2keyNew);
				word.set( String.valueOf(dist)+" "+String.valueOf(gid) );
				//word.set( String.valueOf(dist)+" "+String.valueOf(gid)+" "+String.valueOf(SLl.length)
					//	+" "+String.valueOf(m2key)+" "+String.valueOf(m2keyNew));
				context.write(m2keyOut,word);
			}
		}
		//<key,value> emit done
	}

}

