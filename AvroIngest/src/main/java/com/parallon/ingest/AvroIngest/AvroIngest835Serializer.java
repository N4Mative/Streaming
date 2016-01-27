package com.parallon.ingest.AvroIngest;

import static org.apache.flume.serialization.AvroEventSerializerConfigurationConstants.COMPRESSION_CODEC;
import static org.apache.flume.serialization.AvroEventSerializerConfigurationConstants.DEFAULT_COMPRESSION_CODEC;
import static org.apache.flume.serialization.AvroEventSerializerConfigurationConstants.DEFAULT_SYNC_INTERVAL_BYTES;
import static org.apache.flume.serialization.AvroEventSerializerConfigurationConstants.SYNC_INTERVAL_BYTES;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.HashMap;
/*import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
*/
import java.util.HashSet;
import java.util.Iterator;
//import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.AbstractAvroEventSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Charsets;
//import com.parallon.ingest.Helper.MySQLConnection;

public class AvroIngest835Serializer extends AbstractAvroEventSerializer<AvroIngest835Serializer.Container> {

 
  private static final Schema SCHEMAHL7 = new Schema.Parser().parse(
		  "{ \"type\":\"record\",\"name\":\"Event\",\"fields\":" +
				"[ {\"name\": \"headers\", \"type\": {\"type\":\"map\",\"values\":\"string\",\"default\":\"\"},\"default\":{\"type\":\"map\",\"values\":\"\"}}, " +
				 "{\"name\":\"MSGID\",\"type\":\"string\", \"default\": \"\" }," +
				 "{\"name\":\"MSGDATETIME\",\"type\":\"string\", \"default\": \"\" }," +
				 "{\"name\":\"MSGEVENT\",\"type\":\"string\", \"default\": \"\" }," +
				 "{\"name\":\"version\",\"type\":\"string\", \"default\": \"\" }," +
				 "{\"name\":\"x12Header\", \"type\":{\"type\":\"map\",\"values\":\"string\",\"default\":\"\"},\"default\":{\"type\":\"map\",\"values\":\"\"}}," +
				 "{\"name\":\"x12ST\", \"type\":{\"type\":\"map\",\"values\":\"string\",\"default\":\"\"},\"default\":{\"type\":\"map\",\"values\":\"\"}}," +
				 "{\"name\":\"SEGFIELDS\", \"type\":{\"type\":\"map\",\"values\":\"string\",\"default\":\"\"},\"default\":{\"type\":\"map\",\"values\":\"\"}}" +
				 "]}" );
  
			 
  private final OutputStream out;
  private DatumWriter<Object> writer = null;
  private DataFileWriter<Object> dataFileWriter = null;
  private int syncIntervalBytes;
  private String compressionCodec;
  private int eventCount;
  HTable table = null;
  private AvroIngest835Serializer(OutputStream out) {
    this.out = out;
  }

  @Override
  protected Schema getSchema() {
	//This would return the schema, Hard coded in the code right now, will move to a URL
    return SCHEMAHL7;
  }
  
  @Override
  public void flush() throws IOException {
	 // System.out.println("Flushing the dataFileWriter");
	  System.out.println("I am in flush");
	  dataFileWriter.flush();
  }

  @Override
  public void configure(Context context) {
	System.out.println("I am in configure");
    syncIntervalBytes =
        context.getInteger(SYNC_INTERVAL_BYTES, DEFAULT_SYNC_INTERVAL_BYTES);
    compressionCodec =
        context.getString(COMPRESSION_CODEC, DEFAULT_COMPRESSION_CODEC);
    
    if (dataFileWriter == null) {
	    writer = new GenericDatumWriter<Object>(getSchema());
	    dataFileWriter = new DataFileWriter<Object>(writer);

	    
		dataFileWriter.setSyncInterval(syncIntervalBytes);

	    
	    try {
			dataFileWriter.create(getSchema(), out);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	Configuration conf = HBaseConfiguration.create();
    
	try {
		table = new HTable(conf, "fs");
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

  }
  
  @Override
  public void write(Event event) throws IOException {
	  System.out.println("I am in write");
	  System.out.println("event count - " + eventCount++);
	  String body = new String(event.getBody());
	  
	  GenericRecordBuilder recordBuilder = new GenericRecordBuilder(getSchema());
	  
	  String[] lineParser = body.split("\\r");
	  
		String segSeparator = body.substring(105, 106);
		
		HashMap<String,String> x12Header = new LinkedHashMap<String, String>();
		HashMap<String,String> x12Body = new LinkedHashMap<String, String>();
		HashMap<String,String> x12TR = new LinkedHashMap<String, String>();
		HashMap<String,String> x12ST = new LinkedHashMap<String, String>();
		
		String strISA = body.substring(body.indexOf("ISA*"), body.indexOf(segSeparator+"ST*")+1);
		x12Header.put("RAWHEADER",strISA);
		String[] strHeader = strISA.split(segSeparator);
		for (int i=0;i<strHeader.length;i++){
			String[] strHeadSplit = strHeader[i].split("\\*");
			x12Header.put(strHeadSplit[0] + "_" + strHeadSplit[1], strHeader[i]);
			for (int j=1;j<strHeadSplit.length;j++){
				x12Header.put(strHeadSplit[0] + "_" + strHeadSplit[1] + "_" + j, strHeadSplit[j]);
			}
		}
		
  	//int startPt = body.indexOf("~ST*")+1;
  	int startPt = body.indexOf(segSeparator+"ST*")+1;
  	
		String strBody = body.substring(startPt, body.indexOf(segSeparator+"GE*")+1);
		String strGE = body.substring(body.indexOf(segSeparator+"GE*")+1, body.length());
		x12Header.put("RAWFOOTER",strGE);
		String[] strFooter = strGE.split(segSeparator);
		for (int i=0;i<strFooter.length;i++){
			String[] strFooterSplit = strHeader[i].split("\\*");
			x12Header.put(strFooterSplit[0] + "_" + strFooterSplit[1], strHeader[i]);
			for (int j=1;j<strFooterSplit.length;j++){
				x12Header.put(strFooterSplit[0] + "_" + strFooterSplit[1] + "_" + j, strFooterSplit[j]);
			}
		}
		
		String[] strST = strBody.split(segSeparator+"ST\\*");
		int tranCount=0;
//		for (String strTran : strST){
		for(tranCount=0;tranCount<strST.length;tranCount++){
			x12ST.clear();
			//tranCount++;
			String strTran=strST[tranCount];
			String strTransaction ="";
			if (tranCount==0)
				strTransaction = strTran;
			else
				strTransaction = "ST*"+strTran;
			
			String strTranDetailsRaw ="";
			if (strTransaction.indexOf(segSeparator+"LX*") == -1){
				int intEndOfST = strTran.indexOf(segSeparator+"PLB*"); 
				if (intEndOfST < 0){
					intEndOfST = strTran.indexOf(segSeparator+"SE*");
				}	
				strTranDetailsRaw = strTransaction.substring(0, intEndOfST+1);
			}
			else{
				strTranDetailsRaw = strTransaction.substring(0, strTransaction.indexOf(segSeparator+"LX*")+1);	
			}
				
			
			String[] strTranDetail = strTranDetailsRaw.split(segSeparator);
			x12ST.put("STRAW", strTranDetailsRaw);
			for (int i=0;i<strTranDetail.length;i++){
				String[] strTranDetailSplit = strTranDetail[i].split("\\*");
				x12ST.put(strTranDetailSplit[0] + "_" + strTranDetailSplit[1], strTranDetail[i]);
				for (int j=1;j<strTranDetailSplit.length;j++){
					x12ST.put(strTranDetailSplit[0] + "_" + strTranDetailSplit[1] + "_" + j, strTranDetailSplit[j]);
				}
			}
			
			int intEndOfST = strTran.indexOf(segSeparator+"PLB*"); 
			if (intEndOfST < 0){
				intEndOfST = strTran.indexOf(segSeparator+"SE*");
			}
			String strTranEnd = strTran.substring(intEndOfST+1, strTran.length());
			
			String[] strTranEndDetail = strTranEnd.split(segSeparator);
			x12ST.put("SERAW", strTranEnd);
			for (int i=0;i<strTranEndDetail.length;i++){
				String[] strTranEndDetailSplit = strTranEndDetail[i].split("\\*");
				x12ST.put(strTranEndDetailSplit[0] + "_" + strTranEndDetailSplit[1], strTranEndDetail[i]);
				for (int j=1;j<strTranEndDetailSplit.length;j++){
					x12ST.put(strTranEndDetailSplit[0] + "_" + strTranEndDetailSplit[1] + "_" + j, strTranEndDetailSplit[j]);
				}
			}
			
			
			String[] strLX = strTransaction.split(segSeparator+"LX\\*");
			for (int i=1;i<strLX.length;i++){
		    	String TRs = "LX*"+strLX[i].substring(0,strLX[i].indexOf(segSeparator+"CLP*"));
		    	String[] strTR = TRs.split(segSeparator);
		    	String[] strCLP = strLX[i].split(segSeparator+"CLP\\*");
		    	x12TR.clear();
		    	x12Body.clear();
		    	//Add the TR
		    	for (int k=0;k<strTR.length;k++){
		    		String[] strSplit = strTR[k].split("\\*");
		    		x12TR.put(strSplit[0] + "_" + (i),strTR[k]);
		    		for(int j=1;j<strSplit.length;j++){
		    			x12TR.put(strSplit[0] + "_" + (i) + "_" + j, strSplit[j]);
		    		}
		    	}
		    	String sTS34 = x12TR.get("TS3_"+i+"_4");
		    		//Add the CLP		
	    		for(int l=1;l<strCLP.length;l++){
	    			x12Body.clear();
	    			Iterator<String> keySetIterator = x12Body.keySet().iterator();
	    			x12Body.put("RAWCLP", "CLP*"+strCLP[l]);
	    			String[] strCLEach = ("CLP*"+strCLP[l]).split(segSeparator);
	    			for (int k=0;k<strCLEach.length;k++){
	        			String[] strCL = strCLEach[k].split("\\*");
	        			if (strCL[0].trim().equalsIgnoreCase("CLP")){
	        				x12Body.put(strCL[0], strCLEach[k]);
	        			}else{
	        				x12Body.put(strCL[0]+"_"+strCL[1], strCLEach[k]);	
	        			}
	        			
	        			for(int m=1;m<strCL.length;m++){
	        				if (strCL[0].trim().equalsIgnoreCase("CLP")){
	        					x12Body.put(strCL[0]+"_"+m,strCL[m]);
	        				}else{
	        					x12Body.put(strCL[0]+"_"+strCL[1]+"_"+m,strCL[m]);	
	        				}
	        			}
	    				
	    			}
	    			String sCLP = x12Body.get("CLP_1");
	    			x12Body.putAll(x12TR);
		  			GenericRecord record = recordBuilder.build();
		  			record.put("headers", event.getHeaders());
		  		  //Message headers segements 
		  			record.put("MSGID","");
		  			record.put("MSGDATETIME","");
		  			record.put("MSGEVENT","");
		  			record.put("version","");
		  			record.put("x12Header", x12Header);
		  			record.put("x12ST", x12ST);
		  			record.put("SEGFIELDS",x12Body);
		  			dataFileWriter.append(record);
  			
  			}
	    }
	  
  	}
	  
	  
	  
	  
	  //Write the record to the AVRO
	  
	  
	  
		//This is for HBASE logging
	  Put put = new Put(Bytes.toBytes(getFileType(body) + "_" + event.getHeaders().get("file")));
	  put.add(Bytes.toBytes("fn"), Bytes.toBytes("ST"),
	         Bytes.toBytes("COMPLETED"));
	
	  try {
		table.put(put);
		
	  } catch (RetriesExhaustedWithDetailsException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	  } catch (InterruptedIOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	  }
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
  //This method will creae and return the Map of all the segments and parts of segments
  // that is put as part of the AVRO file
  private Map<String, String> createBodySegments(String[] strRawSplit) {
	  
	  	HashMap<String, String> acMap = new LinkedHashMap<String, String>();
		HashMap<String, Integer> segCounter = new HashMap<String, Integer>();
		String lxCount="";
		for (int i =0; i< strRawSplit.length; i++){
			String[] strAccountNo = strRawSplit[i].split("\\*");
			if (strAccountNo[0].trim().equalsIgnoreCase("LX")){
				for (int j=i;j<strRawSplit.length;j++){
					String[] strSegs = strRawSplit[j].split("\\*");
					if (strSegs[0].trim().equalsIgnoreCase("SE")){
						i=j-1;
						break;
					}
					if (strSegs[0].trim().equalsIgnoreCase("LX")){
						lxCount = strSegs[1];
					}
					acMap.put(strSegs[0] + "_" + lxCount, strRawSplit[j]);
				}
			}
			else /*if (strAccountNo[0].trim().equalsIgnoreCase("HL")){
				
				for (int j=i;j<strRawSplit.length;j++){
					String[] strSegs = strRawSplit[j].split("\\*");
					if (strSegs[0].trim().equalsIgnoreCase("CLM")){
						i=j-1;
						break;
					}
					if (strSegs[0].trim().equalsIgnoreCase("HL")){
						lxCount = strSegs[1];
					}
					acMap.put(strSegs[0] + "_" + lxCount, strRawSplit[j]);
				}
				
			}else*/
				{
				if (strAccountNo[0].trim().equalsIgnoreCase("CLM")){
					if (acMap.containsKey(strAccountNo[0])){
						acMap.remove(strAccountNo[0]);
					}
					Integer segCount = segCounter.get(strAccountNo[0]);
					if(segCount==null){
						segCount = 0;
					}
					++segCount;
					segCounter.put(strAccountNo[0], segCount);
					acMap.put(strAccountNo[0] + "_" + segCount, strRawSplit[i]);
					
				}
				else{
					if (acMap.containsKey(strAccountNo[0] + "_" + strAccountNo[1])){
						acMap.remove(strAccountNo[0] + "_" + strAccountNo[1]);
						//acMap.put()
					}
					Integer segCount = segCounter.get(strAccountNo[0] + "_" + strAccountNo[1]);
					if(segCount==null){
						segCount = 0;
					}
					++segCount;
					segCounter.put(strAccountNo[0] + "_" + strAccountNo[1], segCount);
					acMap.put(strAccountNo[0] + "_" + strAccountNo[1] + "_" + segCount, strRawSplit[i]);
				}
			}
		}
		  
	  return acMap;
}

@Override
  public void afterCreate() throws IOException {
    // write the AVRO container format header
    //System.out.println("I am in 1");
  }
  
  @Override
  protected OutputStream getOutputStream() {
	  //System.out.println("I am in 2	");
    return out;
  }

  @Override
  protected Container convert(Event event) {
	  System.out.println("I am in Convert");
	  return new Container(event.getHeaders(), new String(event.getBody(), Charsets.UTF_8));
  }
  
  public static class Container {
    private final Map<String, String> headers;
    private final String body;
    
    public Container(Map<String, String> headers, String body) {
      super();
      this.headers = headers;
      this.body = body;
    }
  }

  public static class Builder implements EventSerializer.Builder {

    public EventSerializer build(Context context, OutputStream out) {
      AvroIngest835Serializer writer = new AvroIngest835Serializer(out);
      writer.configure(context);
      return writer;
    }

  }

}

