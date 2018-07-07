import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;

import com.espertech.esper.client.EPException;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

public class MainOnibusRecife {
	
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
		
		//String eplOnibusParado = "select * from OnibusEvent match_recognize ( partition by unidade measures A.coordX as a_coordX, A.coordY as a_coordY, B.coordX as b_coordX, B.coordY as b_coordY after match skip to next row pattern (A B) define B as distance(A.coordX, A.coordY, B.coordX, B.coordY) < 2 )";
		//String eplOnibusParado = "select * from OnibusEvent match_recognize ( partition by unidade measures A.coordX as a_coordX, A.coordY as a_coordY, B.coordX as b_coordX, B.coordY as b_coordY after match skip to next row pattern (A B) define B as sqrt((B.coordX - A.coordX)*(B.coordX - A.coordX) + (B.coordY - A.coordY)*(B.coordY - A.coordY)) < 2 )";
		//String eplOnibusParado = "select * from OnibusEvent match_recognize ( partition by unidade measures A.unidade as unidade, A.instante as a_instante, B.instante as b_instante, A.coordX as a_coordX, A.coordY as a_coordY, B.coordX as b_coordX, B.coordY as b_coordY after match skip to next row pattern (A B) define B as ((B.coordX - A.coordX)*(B.coordX - A.coordX) + (B.coordY - A.coordY)*(B.coordY - A.coordY)) < 4 )";
		String eplOnibusParado = "select * from OnibusEvent match_recognize ( partition by unidade measures A.unidade as unidade, A.instante as a_instante, B.instante as b_instante, A.coordX as a_coordX, A.coordY as a_coordY, B.coordX as b_coordX, B.coordY as b_coordY after match skip to next row pattern (A B) define B as ((B.coordX - A.coordX)*(B.coordX - A.coordX) + (B.coordY - A.coordY)*(B.coordY - A.coordY)) < 4 and A.coordX<>0 and A.coordY<>0 and B.coordX<>0 and B.coordY<>0 )";
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
			//System.out.println(String.format("(%d, %d, %d, %d)", a_coordX, a_coordY, b_coordX, b_coordY));
			//System.out.println(String.format("%8s : %s -> %s (%d, %d, %d, %d)", unidade, Globals.dateFormat.format(a_instante), Globals.dateFormat.format(b_instante), a_coordX, a_coordY, b_coordX, b_coordY));
			System.out.println(String.format("%8s : %s (%d, %d)", unidade, Globals.dateFormat.format(a_instante), a_coordX, a_coordY));
			System.out.println(String.format("%8s : %s (%d, %d)", unidade, Globals.dateFormat.format(b_instante), b_coordX, b_coordY));
			System.out.println("--");
			//System.out.println(String.format("e) Veiculo com placa %s aumentando velocidade (de %.1f para %.1f km/h)", placa, velA, velB));
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
		
		OnibusEventThread onibusEventThread = new OnibusEventThread(epr);
		
		onibusEventThread.stop(10000);
	}
	
}
