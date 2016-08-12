import java.io.IOException;
import java.util.HashMap;

public class FeatureDocument implements Comparable<FeatureDocument>{

	HashMap<String,Double> features = new HashMap<>();
	String qry;
	String external_id;
	Integer internal_id;
	double score = 0.0;
	
	public FeatureDocument(String q,String e_id, int i_id){
		this.qry = q;
		this.external_id = e_id;
		this.internal_id = i_id;
		this.addSpam();
		this.addUrlFeatures();
		this.addPagerank();
	}
	private double countSlashes(String s){
		double sum = 0.0;
		for(int i = 0; i < s.length(); i++){
			if(s.charAt(i) == '/'){
				sum++;
			}
		}
		return sum;
	}
	private double isWikipedia(String s){
		double isWiki = 0.0;
		if(s.contains("wikipedia.org")){
			isWiki = 1.0;
		}
		return isWiki;
	}
	private void updateMaxMin(String feature, double value){
		if(value == Double.MAX_VALUE) return;
		if(!QryEval.maxFeatureMap.containsKey(qry)){
			QryEval.maxFeatureMap.put(qry,new HashMap<>());
		}
		if(QryEval.maxFeatureMap.get(qry).containsKey(feature)){
			if(QryEval.maxFeatureMap.get(qry).get(feature) < value){
				QryEval.maxFeatureMap.get(qry).put(feature, value);
			}
		}else{
			QryEval.maxFeatureMap.get(qry).put(feature, value);
		}
		if(!QryEval.minFeatureMap.containsKey(qry)){
			QryEval.minFeatureMap.put(qry,new HashMap<>());
		}
		if(QryEval.minFeatureMap.get(qry).containsKey(feature)){
			if(QryEval.minFeatureMap.get(qry).get(feature) > value){
				QryEval.minFeatureMap.get(qry).put(feature, value);
			}
		}else{
			QryEval.minFeatureMap.get(qry).put(feature, value);
		}
	}
	public void addPagerank(){
		if(QryEval.pagerankMap.containsKey(external_id)){
			this.features.put("f4",QryEval.pagerankMap.get(external_id));
			updateMaxMin("f4",features.get("f4"));
		}
	}
	public void addUrlFeatures(){
		try {
			String url = Idx.getAttribute ("rawUrl", internal_id);
			this.features.put("f2",countSlashes(url));
			updateMaxMin("f2",features.get("f2"));
			this.features.put("f3",isWikipedia(url));
			updateMaxMin("f3",features.get("f3"));
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
	public void addSpam(){
		try {
			this.features.put("f1", Double.parseDouble(Idx.getAttribute ("score", internal_id)));
			updateMaxMin("f1",features.get("f1"));
		} catch (NumberFormatException | IOException e) {
			e.printStackTrace();
		}
	}
	public void addFeatureValue(String feature, Double value){
		this.features.put(feature, value);
		updateMaxMin(feature,value);
	}
	public void setScore(double s){
		this.score = s;
	}
	@Override
	public int compareTo(FeatureDocument o) {
		
		return (o.score > this.score?1:o.score < this.score?-1:0);
	}
	
	
	
}
