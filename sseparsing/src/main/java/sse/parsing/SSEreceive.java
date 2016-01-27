package sse.parsing;

//import javax.ws.rs.Consumes;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
//import javax.ws.rs.core.MediaType;

import org.glassfish.jersey.media.sse.EventInput;
import org.glassfish.jersey.media.sse.EventListener;
import org.glassfish.jersey.media.sse.EventSource;
import org.glassfish.jersey.media.sse.InboundEvent;
import org.glassfish.jersey.media.sse.SseFeature;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class SSEreceive {

    public static void main(String[] args) {
    // TODO Auto-generated method stub
    	try {          
    		Client client = ClientBuilder.newBuilder().register(SseFeature.class).build();

        //String url = "http://www.w3schools.com/html/demo_sse.php";
    		String url = "https://api.particle.io/v1/devices/events?access_token=89e10e9af366d2a916a11d9cfbf5781db7b51234";

    		WebTarget target =   ((Client)client).target(url);

    		EventSource eventSource = (EventSource)EventSource.target(target).build();             

    		EventInput eventInput = target.request().get(EventInput.class);
    		while (!eventInput.isClosed()) {
    			final InboundEvent inboundEvent = eventInput.read();
    			if (inboundEvent == null) {
                // connection has been closed
    				break;
    			} else {
    				if(!inboundEvent.equals(null)) {
    					JsonParser(inboundEvent.readData(String.class));
    					System.out.println(inboundEvent.getName() + "; "
    						+ inboundEvent.readData(String.class));
    				}
    			}
    		}
        /*
        EventListener listener = new EventListener() {
        	//@Override
		 //@Consumes(MediaType.APPLICATION_JSON)
		  public void onEvent(InboundEvent inboundEvent) {      
		  //  System.out.println(inboundEvent.getName() + "; " + inboundEvent.readData(String.class));  
		    System.out.println(inboundEvent.readData(String.class));
		 }
		 };

 //eventSource.register(listener, "message-to-client");

		 eventSource.register(listener);
		 eventSource.open();
		 System.out.println("Connection tried");
		 eventSource.close();
*/
    } catch (ProcessingException pe) {
        pe.printStackTrace();
        System.out.println(pe.getMessage());
    } catch (Exception e) {
        e.printStackTrace();
        System.out.println(e.getMessage());         
    }       
   } 
    
    private static void JsonParser(String s) {
    	JSONParser parser=new JSONParser();

    	  System.out.println("=======decode=======");
    	                
    	  //String s="[0,{\"1\":{\"2\":{\"3\":{\"4\":[5,{\"6\":7}]}}}}]";
    	  Object obj;
		try {
			obj = parser.parse(s);
		
			JSONObject jsonObj = (JSONObject) obj;
			printJsonObject(jsonObj);
 
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    private static void printJsonObject(JSONObject jsonObj) {
        for (Object key : jsonObj.keySet()) {
            //based on you key types
            String keyStr = (String)key;
            Object keyvalue = jsonObj.get(keyStr);

            //Print key and value
            System.out.println(keyStr + " : " + keyvalue);

            //for nested objects iteration if required
            if (keyvalue instanceof JSONObject)
                printJsonObject((JSONObject)keyvalue);
        }
    }
    
  }