package com.parallon.ingest.AvroIngest;


import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;


public class IngestHbaseInterceptor implements Interceptor {
	@Override
	public void initialize() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Event intercept(Event event) {
		// TODO Auto-generated method stub
		Map<String, String> headers = event.getHeaders();
		String Filename = headers.get("file");
		String fileType = getFileType(new String(event.getBody()));
		Configuration conf = HBaseConfiguration.create();
        HTable table = null;
		try {
			table = new HTable(conf, "fs");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Put put = new Put(Bytes.toBytes(fileType + "_" + Filename));
        put.add(Bytes.toBytes("fn"), Bytes.toBytes("ST"),
                 Bytes.toBytes("PICKED"));
        
        try {
			table.put(put);
		} catch (RetriesExhaustedWithDetailsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
		
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return event;
	}

	private String getFileType(String eventBody) {
		String fileType = "";
		if (eventBody.substring(0, 3).equalsIgnoreCase("MSH")){
			fileType = "HL7";
		}else{
			String strFileType = eventBody.substring(eventBody.indexOf("GS*")+3,eventBody.indexOf("*", eventBody.indexOf("GS*")+3));
			if (strFileType.trim().equalsIgnoreCase("HC"))
				fileType = "837";
			else
				fileType = "835";
		}
		
		return fileType;
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		// TODO Auto-generated method stub
	    for (Event event : events) {
		      intercept(event);
		    }
		    return events;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	public static class Builder implements Interceptor.Builder {
		 
	    @Override
	    public Interceptor build() {
	      return new IngestHbaseInterceptor();
	    }
	 
	    @Override
	    public void configure(Context context) {}
	  }
}
