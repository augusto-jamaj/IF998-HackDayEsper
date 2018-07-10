import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeoutException;

import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class MainOnibusRecife {
	
	public static void main(String[] args) {
		//SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		SimpleDateFormat dateFormat = Globals.dateFormat;
		
		EPServiceProvider engine = EPServiceProviderManager.getDefaultProvider();
		engine.getEPAdministrator().getConfiguration().addEventType(OnibusEvent.class);
		
		String SEND_QUEUE_NAME = "esper";
		Globals.channel = null;
		
		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("172.22.43.58");
			factory.setUsername("caio2");
			factory.setPassword("caio");
			Connection connection = factory.newConnection();
			Globals.channel = connection.createChannel();
			
			Globals.channel.queueDeclare(SEND_QUEUE_NAME, false, false, false, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Globals.id = 0;

		//String eplOnibusParado = "select * from OnibusEvent match_recognize ( partition by unidade measures A.coordX as a_coordX, A.coordY as a_coordY, B.coordX as b_coordX, B.coordY as b_coordY after match skip to next row pattern (A B) define B as distance(A.coordX, A.coordY, B.coordX, B.coordY) < 2 )";
		//String eplOnibusParado = "select * from OnibusEvent match_recognize ( partition by unidade measures A.coordX as a_coordX, A.coordY as a_coordY, B.coordX as b_coordX, B.coordY as b_coordY after match skip to next row pattern (A B) define B as sqrt((B.coordX - A.coordX)*(B.coordX - A.coordX) + (B.coordY - A.coordY)*(B.coordY - A.coordY)) < 2 )";
		//String eplOnibusParado = "select * from OnibusEvent match_recognize ( partition by unidade measures A.unidade as unidade, A.instante as a_instante, B.instante as b_instante, A.coordX as a_coordX, A.coordY as a_coordY, B.coordX as b_coordX, B.coordY as b_coordY after match skip to next row pattern (A B) define B as ((B.coordX - A.coordX)*(B.coordX - A.coordX) + (B.coordY - A.coordY)*(B.coordY - A.coordY)) < 4 )";
		String eplOnibusParado = "select * from OnibusEvent match_recognize ( partition by unidade measures A.unidade as unidade, A.instante as a_instante, B.instante as b_instante, A.coordX as a_coordX, A.coordY as a_coordY, B.coordX as b_coordX, B.coordY as b_coordY, B.latitude as b_latitude, B.longitude as b_longitude after match skip to next row pattern (A B) define B as ((B.coordX - A.coordX)*(B.coordX - A.coordX) + (B.coordY - A.coordY)*(B.coordY - A.coordY)) < 4 and A.coordX<>0 and A.coordY<>0 and B.coordX<>0 and B.coordY<>0 )";
		EPStatement statementOnibusParado = engine.getEPAdministrator().createEPL(eplOnibusParado);
		statementOnibusParado.addListener( (newData, oldData) -> {
			String unidade = (String) newData[0].get("unidade");
			Date a_instante = (Date) newData[0].get("a_instante");
			Date b_instante = (Date) newData[0].get("b_instante");
			long a_coordX = (long) newData[0].get("a_coordX");
			long a_coordY = (long) newData[0].get("a_coordY");
			long b_coordX = (long) newData[0].get("b_coordX");
			long b_coordY = (long) newData[0].get("b_coordY");
			double b_latitude = (double) newData[0].get("b_latitude");
			double b_longitude = (double) newData[0].get("b_longitude");
			
			System.out.println(String.format("%8s : %s (%d, %d)", unidade, Globals.dateFormat.format(a_instante), a_coordX, a_coordY));
			System.out.println(String.format("%8s : %s (%d, %d)", unidade, Globals.dateFormat.format(b_instante), b_coordX, b_coordY));
			System.out.println("--");
			
			Globals.id += 1;
			
			String message = String.format("{\"Id\": " + Globals.id + ", \"Latitude\": " + b_latitude + ", \"Longitude\": " + b_longitude + ", \"Descricao\": \"" + unidade + "\"}");
			try {
				System.out.println(message);
				Globals.channel.basicPublish("", SEND_QUEUE_NAME, null, message.getBytes());
				System.out.println(" [x] Sent '" + message + "'");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});		

		
		EPRuntime epr = engine.getEPRuntime();
		
		OnibusEventThread onibusEventThread = new OnibusEventThread(epr);
		
		// Fica rodando durante 100 segundos
		//onibusEventThread.stop(100000);
		
	}
	
}
