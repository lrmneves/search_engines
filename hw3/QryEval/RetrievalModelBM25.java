import java.util.HashMap;

/**
 *  Copyright (c) 2016, Carnegie Mellon University.  All Rights Reserved.
 */

/**
 *  An object that stores parameters for the BM25
 *  retrieval model and indicates to the query
 *  operators how the query should be evaluated.
 */
public class RetrievalModelBM25 extends RetrievalModel {

	private double k_1 = 0.0;
	private double k_3 = 0.5;
	private double b = 0.0;
	private HashMap<String,Double> averageDocLenMap = new HashMap<>();

	public RetrievalModelBM25(double k_1,double k_3,double b){
		this.k_1 = k_1;
		this.k_3 = k_3;
		this.b = b;
	}
	public String defaultQrySopName () {
		return new String ("#sum");
	}
	
	public double getK1(){
		return this.k_1;
	}
	public double getK3(){
		return this.k_3;
	}
	public double getB(){
		return this.b;
	}
	public void addAverageDocLen(String field, double len){
		averageDocLenMap.put(field, len);
	}
	public boolean containsField(String field){
		return averageDocLenMap.containsKey(field);
	}
	public double getAverageDocLen(String field){
		return averageDocLenMap.get(field);
	}
}
