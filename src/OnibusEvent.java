import java.util.Date;

public class OnibusEvent {
	private String unidade;
	private String nome;
	//private String matricula;
	private Date instante;
	//private long estado;
	//private long comunica;
	private long coordX;
	private long coordY;
	//private String linha;
	//private String rota;
	//private String posicao;
	//private String viagem;
	//private String velocidade;
	private double latitude;
	private double longitude;
	
	public OnibusEvent(String unidade, String nome, Date instante, long coordX, long coordY, double latitude, double longitude) {
		this.unidade = unidade;
		this.nome = nome;
		this.instante = instante;
		this.coordX = coordX;
		this.coordY = coordY;
		this.latitude = latitude;
		this.longitude = longitude;
	}
	
	public String getUnidade() {
		return unidade;
	}
	public String getNome() {
		return nome;
	}
	public Date getInstante() {
		return instante;
	}
	public long getCoordX() {
		return coordX;
	}
	public long getCoordY() {
		return coordY;
	}
	public double getLatitude() {
		return latitude;
	}
	public double getLongitude() {
		return longitude;
	}
	
}
