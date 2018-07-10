import java.text.SimpleDateFormat;

import com.rabbitmq.client.Channel;

public class Globals {
	public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	public static final String QUEUE_NAME = "publisher";
	public static int id;
	public static Channel channel;
}
