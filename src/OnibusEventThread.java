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
	
	//private Random rand;
	private EPRuntime epRuntime;
	private boolean running;
	private Thread thread;
	
	public OnibusEventThread(EPRuntime epRuntime) {
		//this.rand = new Random();
		this.epRuntime = epRuntime;
		this.running = true;
		this.thread = new Thread(this);
		this.thread.start();
	}

	@Override
	public void run() {
		//String csvFile = "/Users/jamaj/matrix/2018-1/CIn/SistDist/HackDay/instrucoes/amostra_dados_onibus_recife.csv";
		String csvFile = "/Users/jamaj/matrix/2018-1/CIn/SistDist/HackDay/instrucoes/amostra_dados_onibus_recife_sorted.csv";
		//BufferedReader br = null;
		String line = "";
		String csvSplitBy = ";";
		JsonParser parser = new JsonParser();
		
		try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
			while (running) {
				try {
					Thread.sleep(10000);
					
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

								epRuntime.sendEvent(new OnibusEvent(unidade, nome, instante, coordX, coordY));
							} catch (ParseException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}

						}
					};
					
					channel.basicConsume(Globals.QUEUE_NAME, true, consumer);
					
//					while ((line = br.readLine()) != null) {
//						try {
//							String[] dadosEvento = line.split(csvSplitBy);
//							String unidade = dadosEvento[0];
//							String nome = dadosEvento[1];
//							Date instante = Globals.dateFormat.parse(dadosEvento[3]);
//							long coordX = Long.parseLong(dadosEvento[6]);
//							long coordY = Long.parseLong(dadosEvento[7]);
//						
//							epRuntime.sendEvent(new OnibusEvent(unidade, nome, instante, coordX, coordY));
//						} catch (ParseException e) {
//							e.printStackTrace();
//						}
//					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					running = false;
				} catch (TimeoutException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
//		while (running) {
//			try {
//				Thread.sleep(10);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//				running = false;
//			}
//			//epRuntime.sendEvent(new OnibusEventEvent(10f + 20f*rand.nextFloat()));
//			//epRuntime.sendEvent(new OnibusEvent());
//		}
	}
	
	public void stop(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.running = false;
	}
	
}
