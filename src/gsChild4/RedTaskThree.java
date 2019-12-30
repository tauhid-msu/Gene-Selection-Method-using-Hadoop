package gsChild4;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.lang.StringUtils;

public class RedTaskThree extends Reducer<LongWritable,Text,IntWritable,Text> {
	
	private IntWritable SlKey = new IntWritable();
	private Text word = new Text();
	public static final int ASC = 1;
	public static final int DESC = 2;
	
	public void reduce(LongWritable key, Iterable<Text> value, Context context)
					throws IOException, InterruptedException {
		
		int trNum,Tj,k=0;
		String[] line;
		
		Configuration conf=context.getConfiguration();
		trNum = conf.getInt("trNum", 20);
		String[] smp_cls = StringUtils.split(conf.get("smpls"),"=");
		
		
		double[] kClsifier = new double[trNum]; 
		int[] clsIndex = new int[trNum]; 
		String pSetF = "";
		List<Integer> clsIndexList = new ArrayList<Integer>();
		List<Double> kClsList = new ArrayList<Double>();
		
		for (Text val : value) {
			
			line = StringUtils.split(val.toString(),' ');
			
			
			/*if(!kClsList.contains(Double.parseDouble(line[0]) )){
				kClsList.add( Double.parseDouble(line[0]) );
			}*/
			
			kClsifier[k] = Double.parseDouble(line[0]); 
			
			/*if(!clsIndexList.contains(Integer.parseInt(line[1]) )){
				clsIndexList.add( Integer.parseInt(line[1]) );
			}*/
			clsIndex[k] = Integer.parseInt(line[1]);
			pSetF = line[2];
			k++;
						
		}
		Tj = (int)( key.get() % 1000);
		SlKey.set((int)( key.get() / 1000) );
		
		//clsIndex = convertIntegers(clsIndexList);
		//kClsifier = convertDouble(kClsList);
		
		//check if correctly classfied Tj by kNN Majority label
		//double[] TjArr = new double[trNum];
		//int[] indexOrig = new int[trNum];
		//System.arraycopy(kClsifier,0,TjArr,0,trNum);
		//System.arraycopy(indexOrig,0,clsIndex,0,trNum);
		
		//int cClassify = fitnessCal(TjArr,clsIndex,Integer.parseInt(smp_cls[Tj+trNum-1]));
		int cClassify = fitnessCal(kClsifier,clsIndex,Integer.parseInt(smp_cls[Tj+trNum-1]));
		
		if(cClassify>0){
			//word.set(String.valueOf(cClassify)+" || "+smp_cls[Tj+trNum-1]+" || "+implodeArray(clsIndex,",")
				//		+" || "+implodeArrayDbl(TjArr,",")+" || "+implodeArrayDbl(kClsifier,",")+" || "+pSetF);
			word.set(String.valueOf(cClassify)+" "+pSetF);
			context.write(SlKey,word);
			
		}
			
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
	
	public static double[] convertDouble(List<Double> doubles)	{
		double[] ret = new double[doubles.size()];
	    Iterator<Double> iterator = doubles.iterator();
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

	public static String implodeArrayDbl(double[] inputArray, String glueString) {

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

	public static int fitnessCal(double[] TRij, int[] cls, int exCls){
		
		int l,kVal;
		
		quicksort(TRij,cls,ASC);
		
		for(l=0,kVal=0; l<5; l++){
			if(cls[l] == exCls){
				kVal += 1;
			}
			else{
				kVal -= 1;
			}
		}
		return (kVal>0?1:0);
	}
	
	public static void quicksort(double[] main, int[] index,int order) {
	    quicksort(main, index, 0, index.length - 1,order);
	}

	// quicksort a[left] to a[right]
	public static void quicksort(double[] a, int[] index, int left, int right,int order) {
	    if (right <= left) return;
	    int i = partition(a, index, left, right,order);
	    quicksort(a, index, left, i-1,order);
	    quicksort(a, index, i+1, right,order);
	}

	// partition a[left] to a[right], assumes left < right
	private static int partition(double[] a, int[] index, int left, int right,int order) {
	    int i = left - 1;
	    int j = right;
	    if(order==ASC){
		    while (true) {
		        while (less(a[++i], a[right]))      // find item on left to swap
		            ;                               // a[right] acts as sentinel
		        while (less(a[right], a[--j]))      // find item on right to swap
		            if (j == left) break;           // don't go out-of-bounds
		        if (i >= j) break;                  // check if pointers cross
		        exch(a, index, i, j);               // swap two elements into place
		    }
	    }
	    else if(order==DESC){
		    while (true) {
		        while (more(a[++i], a[right]))      // find item on left to swap
		            ;                               // a[right] acts as sentinel
		        while (more(a[right], a[--j]))      // find item on right to swap
		            if (j == left) break;           // don't go out-of-bounds
		        if (i >= j) break;                  // check if pointers cross
		        exch(a, index, i, j);               // swap two elements into place
		    }
		    //exch(a, index, i, left);               // swap with partition element
	    }
	    exch(a, index, i, right);               // swap with partition element
	    return i;
	}

	// is x < y ?
	private static boolean less(double x, double y) {
	    return (x < y);
	}
	private static boolean more(double x, double y) {
	    return (y < x);
	}
	private static void exch(double[] a, int[] index, int i, int j) {
		double swap = a[i];
	    a[i] = a[j];
	    a[j] = swap;
	    int b = index[i];
	    index[i] = index[j];
	    index[j] = b;
	}
	
	
}
