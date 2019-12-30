package gsChild4;

import java.io.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;

public class MapTaskThree extends Mapper<LongWritable, Text, LongWritable,Text>{
	
	private Text word = new Text();
	private LongWritable m2keyOut = new LongWritable();
	
	public void map(LongWritable key, Text values, Context context)
			throws IOException, InterruptedException {
		String line = values.toString();
		
		int Tri,j;
		long m2key=0;
		String dSum="",gList="";
		
		Configuration conf =context.getConfiguration();
		String[] smpCls = StringUtils.split(conf.get("smpls"),"=");
		
		//updated code from this point
		StringTokenizer tokenizer = new StringTokenizer(line);
		j=0;
		while (tokenizer.hasMoreTokens()) {
		
			String test = tokenizer.nextToken();
			if(j==0){
				m2key = Long.parseLong(test);
			}
			else if(j==1){
				dSum = test;
			}
			else if(j==2){
				gList = test;
			}
			j++;
		}
		//Map input ends
		
		//remove Tr-id from key 
		Tri = (int)(m2key % 1000);
		m2key = m2key / 1000;
		
		//Map2 <key,value> emit starts
		if(m2key>0){
			m2keyOut.set(m2key);
			word.set( dSum+" "+String.valueOf(smpCls[Tri-1])+" "+gList );
			context.write(m2keyOut,word);
		}
		//<key,value> emit done
	}

}
