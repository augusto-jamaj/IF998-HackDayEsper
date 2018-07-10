import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

import com.espertech.esper.client.EPRuntime;
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

public class OnibusEventThread implements Runnable {
	
	private EPRuntime epRuntime;
	private boolean running;
	private Thread thread;
	
	public OnibusEventThread(EPRuntime epRuntime) {
		this.epRuntime = epRuntime;
		this.running = true;
		this.thread = new Thread(this);
		this.thread.start();
	}

	@Override
	public void run() {
		JsonParser parser = new JsonParser();
		
		while (running) {
			try {
				ConnectionFactory factory = new ConnectionFactory();
				factory.setHost("172.22.43.58");
				factory.setUsername("caio2");
				factory.setPassword("caio");
				Connection connection = factory.newConnection();
				Channel channel = connection.createChannel();
				
				channel.queueDeclare(Globals.QUEUE_NAME, false, false, false, null);
				System.out.println(" [*] Esperando mensagens...");
				
				Consumer consumer = new DefaultConsumer(channel) {
					public void handleDelivery(String consumerTag, Envelope envelope,
							AMQP.BasicProperties properties, byte[] body)
									throws IOException {
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
				
				Thread.sleep(10);
				
				connection.close();
				
				} catch (Exception e) {
					e.printStackTrace();
				}
		}

	}
	
	public void stop(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		this.running = false;
	}
	
}
