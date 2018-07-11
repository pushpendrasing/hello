package abc;

import java.io.*;

import java.util.*;
import java.util.Map.Entry;


//public class com implements Comparable<Map.Entry<Integer,String>>{

	//@Override
	//public int compareTo(Entry<Integer, String> e) {
		// TODO Auto-generated method stub
		
		//return this.;
	//}}
public class Solution {
	
	/*public static <T, E> Set<T> getKeysByValue(Map<T, E> map, E value) {
	    Set<T> keys = new HashSet<T>();
	    for (Entry<T, E> entry : map.entrySet()) {
	        if (Objects.equals(value, entry.getValue())) {
	            keys.add(entry.getKey());
	        }
	    }
	    return keys;
	}*/

    public static void main(String[] args) 
    {
    	
    	Map<Integer,String> m1=new TreeMap<>();
    	m1.put(5, "pushpendra");
    	m1.put(4,"ram");
    	m1.put(3,"vikas");
    	m1.put(1,"nishant");
    	m1.put(2,"ram");
    	
    	Set s=m1.entrySet();
    	Iterator iter = s.iterator();

    	while (iter.hasNext()) 
    	{
    		Map.Entry entry = (Map.Entry) iter.next();
    	    System.out.println(entry.getKey() + " -- " + entry.getValue());
    	}
    	
    	//Map<Integer,String> m2=new LinkedHashMap<>();
    	//ArrayList<String>  val=new ArrayList<>();
    	
    		Set<String> val=new TreeSet<String>(m1.values());
    		iter=val.iterator();
    		while(iter.hasNext())
    	    {
    	    	System.out.println(iter.next());
    	    }
    		
    		HashSet<Integer> keys = new HashSet<Integer>();
    	    for(String v:val)
    	    {
    	    	 //System.out.println("hello moto");
    		    for (Map.Entry<Integer,String> entry : m1.entrySet()) {
    		    	//System.out.println("hello moto");
    		        if (Objects.equals(v, entry.getValue())) {
    		            keys.add(entry.getKey());
    		            //System.out.println("hello moto");
    		        }
    		    }
    		    	
    	  /* for(Integer i:getKeysByValue(m1,itr.next()));
    	   {
    		   Integer j=i.
    		   m2.put(i,val1 );
    		   
    	   }*/
    	    }
    	    iter=keys.iterator();
    	    while(iter.hasNext())
    	    {
    	    	System.out.println(iter.next());
    	    }
    	}
    }
