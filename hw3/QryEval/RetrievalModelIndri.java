/**
 *  Copyright (c) 2016, Carnegie Mellon University.  All Rights Reserved.
 */

/**
 *  An object that stores parameters for the Indri
 *  retrieval model and indicates to the query
 *  operators how the query should be evaluated.
 */
public class RetrievalModelIndri extends RetrievalModel {

	int mu = 2500;
	double lambda = 0.4;
	int currentDoc = 0;


	public RetrievalModelIndri(int mu, double lambda){
		this.mu = mu;
		this.lambda = lambda;
	}

	public String defaultQrySopName () {
		return new String ("#and");
	}

}
