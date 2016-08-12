/** 
 *  Copyright (c) 2016, Carnegie Mellon University.  All Rights Reserved.
 */

/**
 *  The root class in the retrieval model hierarchy.  This hierarchy
 *  is used to create objects that provide fast access to retrieval
 *  model parameters and indicate to the query operators how the query
 *  should be evaluated.
 */
public abstract class RetrievalModel {
	private boolean letor = false;
  /**
   *  The name of the default query operator for the retrieval model.
   *  @return The name of the default query operator.
   */
  public abstract String defaultQrySopName ();
  public void setLetor(){
		this.letor = true;
	}
	public boolean isLetor(){
		return this.letor;
	}
  
}
