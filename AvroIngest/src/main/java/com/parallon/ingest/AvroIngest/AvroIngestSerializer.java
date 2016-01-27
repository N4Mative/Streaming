package com.parallon.ingest.AvroIngest;

import static org.apache.flume.serialization.AvroEventSerializerConfigurationConstants.COMPRESSION_CODEC;
import static org.apache.flume.serialization.AvroEventSerializerConfigurationConstants.DEFAULT_COMPRESSION_CODEC;
import static org.apache.flume.serialization.AvroEventSerializerConfigurationConstants.DEFAULT_SYNC_INTERVAL_BYTES;
import static org.apache.flume.serialization.AvroEventSerializerConfigurationConstants.SYNC_INTERVAL_BYTES;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
/*import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
*/
import java.util.HashSet;
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

public class AvroIngestSerializer extends AbstractAvroEventSerializer<AvroIngestSerializer.Container> {

 
  private static final Schema SCHEMAHL7 = new Schema.Parser().parse(
		  "{ \"type\":\"record\",\"name\":\"Event\",\"fields\":" +
				"[ {\"name\": \"headers\", \"type\": {\"type\":\"map\",\"values\":\"string\",\"default\":\"\"},\"default\":{\"type\":\"map\",\"values\":\"\"}}, " +
				 "{\"name\":\"MSGID\",\"type\":\"string\", \"default\": \"\" }," +
				 "{\"name\":\"MSGDATETIME\",\"type\":\"string\", \"default\": \"\" }," +
				 "{\"name\":\"MSGEVENT\",\"type\":\"string\", \"default\": \"\" }," +
				 "{\"name\":\"version\",\"type\":\"string\", \"default\": \"\" }," +
				 "{\"name\":\"SEGFIELDS\", \"type\":{\"type\":\"map\",\"values\":\"string\",\"default\":\"\"},\"default\":{\"type\":\"map\",\"values\":\"\"}}" +
				 "]}" );
  
			 
  private final OutputStream out;
  private DatumWriter<Object> writer = null;
  private DataFileWriter<Object> dataFileWriter = null;
  private static final Set<String> segmentsRepeating = new HashSet<String>();
  private int syncIntervalBytes;
  private String compressionCodec;
  HTable table = null;
  private AvroIngestSerializer(OutputStream out) {
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
	  dataFileWriter.flush();
  }

  @Override
  public void configure(Context context) {

    syncIntervalBytes =
        context.getInteger(SYNC_INTERVAL_BYTES, DEFAULT_SYNC_INTERVAL_BYTES);
    compressionCodec =
        context.getString(COMPRESSION_CODEC, DEFAULT_COMPRESSION_CODEC);
    
    segmentsRepeating.add("OBX");
    segmentsRepeating.add("IN2");
    segmentsRepeating.add("IN1");
    segmentsRepeating.add("ROL");
    segmentsRepeating.add("AL1");
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
	  
	  String body = new String(event.getBody());
	  
	  GenericRecordBuilder recordBuilder = new GenericRecordBuilder(getSchema());
	  GenericRecord record = recordBuilder.build();
	  String[] HL7LineParse = body.split("\\r");
	  record.put("headers", event.getHeaders());
      
	  //Map variable to store the message body segments
      Map<String,String> bodySegments = new LinkedHashMap<String, String>();
      bodySegments = createBodySegments(HL7LineParse);
      
      //TODO: Remove, this is for monitoring the body segments
      /*Iterator<String> keySetIterator = bodySegments.keySet().iterator();
	  	while(keySetIterator.hasNext()){
	  	  String key = keySetIterator.next();
	  	  System.out.println("key: " + key + " value: " + bodySegments.get(key));
	  	}
      */

      //Message headers segements 
      String[] parsedHL7 = HL7LineParse[0].split("\\|");
      record.put("MSGID",parsedHL7[9]);
      record.put("MSGDATETIME",parsedHL7[6]);
      record.put("MSGEVENT",parsedHL7[8]);
      record.put("version",parsedHL7[11]);
      record.put("SEGFIELDS",bodySegments);
      
      //Write the record to the AVRO
      dataFileWriter.append(record);
      //SXP - This is for logging to MySQL if we decide doing that, otherwise remove it
      /*new MySQLConnection();
		Connection conn = null;
		try {
			conn = MySQLConnection.GetCon("");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			Statement st = conn.createStatement();
			String SQL = "update ExceptionTable set FileStatus = 'COMPLETE' where FileName = '" + event.getHeaders().get("file") + "' and MessageType = 'HL7'";
			st.executeUpdate(SQL);
			st.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
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
		/*try {
		
			table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
*/
   
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
  private Map<String, String> createBodySegments(String[] hL7LineParse) {
	  
	  Map<String,String> segments = new LinkedHashMap<String, String>();
	  
	  //Loop on the Line level
	  for (int i=0;i<hL7LineParse.length;i++){

		  //Parse each line on "|" 
		  String[] parsedSegment = hL7LineParse[i].split("\\|");
		  String segName = "";
		  
		  //If the segment is MSH or EVN do not use the first Item as the line segment name
		  if (segmentsRepeating.contains(parsedSegment[0])){
			  segName = parsedSegment[0].trim() + parsedSegment[1].trim();
		  }else{
			  segName = parsedSegment[0].trim();
		  }
		  
		  //Add a Key in the MAP for the line segment
		  if (!hL7LineParse[i].isEmpty())
			  segments.put(segName,hL7LineParse[i]);
		  
		  //Loop on each part of the Line segment split by "|"
		  for (int j=1; j< parsedSegment.length;j++){
			  String fieldName = "";
			  String fieldVal = "";
			  if (parsedSegment[0].equalsIgnoreCase("MSH"))
				  fieldName = segName.trim() + "_" + (j-1);
			  else
				  fieldName = segName.trim() + "_" + j;
			  
			  fieldVal = parsedSegment[j];
			  
			  String[] parseField = fieldVal.split("\\^");
			  
			  //Add a key in the MAP for each part
			  if (!fieldVal.isEmpty())
				  segments.put(fieldName, fieldVal);
			  
			  //Check if the field has subparts based on "^"
			  if (parseField.length > 1){
				  
				  //Loop on each subpart  of the part
				  for (int k=0; k<parseField.length;k++){
					  String subFieldName = fieldName + "_" + (k+1);
					  fieldVal = parseField[k];
					  
					  //Add a key for each subpart
					  if (!fieldVal.isEmpty())
						  segments.put(subFieldName, fieldVal);
				  }
			  }
		  }
	  }
		  
	  return segments;
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
      AvroIngestSerializer writer = new AvroIngestSerializer(out);
      writer.configure(context);
      return writer;
    }

  }

}

