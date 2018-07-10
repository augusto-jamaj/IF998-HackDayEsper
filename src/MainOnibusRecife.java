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
	
	public static void receive(EPRuntime epRuntime) throws IOException, TimeoutException { 
		JsonParser parser = new JsonParser();
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("172.22.43.58");
		//factory.setHost("localhost");
		factory.setUsername("caio2");
		factory.setPassword("caio");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		channel.queueDeclare(Globals.QUEUE_NAME, false, false, false, null);
		System.out.println(" [*] Esperando mensagens...");
		
		Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
				String message = new String(body, "UTF-8");
				//System.out.println(" [x] Received '" + message + "'");
				
				JsonElement element = parser.parse(message);
				JsonObject jsonObject = element.getAsJsonObject();
				
				try {
					String unidade = jsonObject.get("Unidade").getAsString();
					String nome = jsonObject.get("nome").getAsString();
					Date instante = Globals.dateFormat.parse(jsonObject.get("Instante").getAsString());
					long coordX = Long.parseLong(jsonObject.get("CoordX").getAsString());
					long coordY = Long.parseLong(jsonObject.get("CoordY").getAsString());
					double latitude = Double.parseDouble(jsonObject.get("x").getAsString());
					double longitude = Double.parseDouble(jsonObject.get("y").getAsString());
					
					System.out.println(" [*] Mensagem recebida: " + message);
					epRuntime.sendEvent(new OnibusEvent(unidade, nome, instante, coordX, coordY, latitude, longitude));
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		};
		
		channel.basicConsume(Globals.QUEUE_NAME, true, consumer);
	}
	
	public static void main(String[] args) {
		//SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		SimpleDateFormat dateFormat = Globals.dateFormat;
		
		EPServiceProvider engine = EPServiceProviderManager.getDefaultProvider();
		engine.getEPAdministrator().getConfiguration().addEventType(OnibusEvent.class);
		
//		String epl = "select * from OnibusEvent";
//		EPStatement statement = engine.getEPAdministrator().createEPL(epl);
//		statement.addListener( (newData, oldData) -> {
//			String unidade = (String) newData[0].get("unidade");
//			String nome = (String) newData[0].get("nome");
//			Date instante = (Date) newData[0].get("instante");
//			long coordX = (long) newData[0].get("coordX");
//			long coordY = (long) newData[0].get("coordY");
//			
//			System.out.println(String.format("OnibusEvent(%s, %s, %s, %d, %d)", unidade, nome, dateFormat.format(instante), coordX, coordY));
//		});
		
		String SEND_QUEUE_NAME = "esper";
		Globals.channel = null;
		
		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("172.22.43.58");
//			factory.setHost("localhost");
			factory.setUsername("caio2");
			factory.setPassword("caio");
			Connection connection = factory.newConnection();
			Globals.channel = connection.createChannel();
			
			Globals.channel.queueDeclare(SEND_QUEUE_NAME, false, false, false, null);
			//String message = "Hello World!";
			//Globals.channel.basicPublish("", SEND_QUEUE_NAME, null, message.getBytes());
			//System.out.println(" [x] Sent '" + message + "'");
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
			//String placa = (String) newData[0].get("placa");
			String unidade = (String) newData[0].get("unidade");
			Date a_instante = (Date) newData[0].get("a_instante");
			Date b_instante = (Date) newData[0].get("b_instante");
			long a_coordX = (long) newData[0].get("a_coordX");
			long a_coordY = (long) newData[0].get("a_coordY");
			long b_coordX = (long) newData[0].get("b_coordX");
			long b_coordY = (long) newData[0].get("b_coordY");
			double b_latitude = (double) newData[0].get("b_latitude");
			double b_longitude = (double) newData[0].get("b_longitude");
			
			//System.out.println(String.format("(%d, %d, %d, %d)", a_coordX, a_coordY, b_coordX, b_coordY));
			//System.out.println(String.format("%8s : %s -> %s (%d, %d, %d, %d)", unidade, Globals.dateFormat.format(a_instante), Globals.dateFormat.format(b_instante), a_coordX, a_coordY, b_coordX, b_coordY));
			System.out.println(String.format("%8s : %s (%d, %d)", unidade, Globals.dateFormat.format(a_instante), a_coordX, a_coordY));
			System.out.println(String.format("%8s : %s (%d, %d)", unidade, Globals.dateFormat.format(b_instante), b_coordX, b_coordY));
			System.out.println("--");
//			String message = "teste";
//			try {
//				Globals.channel.basicPublish("", SEND_QUEUE_NAME, null, message.getBytes());
//			} catch (IOException e1) {
//				// TODO Auto-generated catch block
//				e1.printStackTrace();
//			}
//			System.out.println(" [x] Sent '" + message + "'");
			
			Globals.id += 1;
			
			//message = String.format("{\"Id\": %d, \"Latitude\": %f, \"Longitude\": %f, \"Descricao\": \"%s\"}", Globals.id, b_coordX, b_coordY, unidade);
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
		//SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		//epr.sendEvent(new OnibusEvent("1111", "E1", Date.from(Instant.parse("2017-11-01 01:32:39.050")), 0, 0));

//		try {
//			epr.sendEvent(new OnibusEvent("1111", "E1", dateFormat.parse("2017-11-01 01:32:39.050"), 0, 0));
//			epr.sendEvent(new OnibusEvent("1111", "E1", dateFormat.parse("2017-11-01 01:33:38.843"), 0, 0));
//		} catch (EPException | ParseException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
//		try {
//			receive(epr);
//		} catch (IOException | TimeoutException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		OnibusEventThread onibusEventThread = new OnibusEventThread(epr);
		
		onibusEventThread.stop(100000);
		
	}
	
}
