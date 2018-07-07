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
	
	public OnibusEvent(String unidade, String nome, Date instante, long coordX, long coordY) {
		this.unidade = unidade;
		this.nome = nome;
		this.instante = instante;
		this.coordX = coordX;
		this.coordY = coordY;
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
	
}
