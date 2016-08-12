/**
 *  Copyright (c) 2016, Carnegie Mellon University.  All Rights Reserved.
 */

import java.io.*;
import java.lang.IllegalArgumentException;
/**
 *  The SCORE operator for all retrieval models.
 */
public class QrySopScore extends QrySop {

	/**
	 *  Document-independent values that should be determined just once.
	 *  Some retrieval models have these, some don't.
	 */

	/**
	 *  Indicates whether the query has a match.
	 *  @param r The retrieval model that determines what is a match
	 *  @return True if the query matches, otherwise false.
	 */
	public boolean docIteratorHasMatch (RetrievalModel r) {
		return this.docIteratorHasMatchFirst (r);
	}

	/**
	 *  Get a score for the document that docIteratorHasMatch matched.
	 *  @param r The retrieval model that determines how scores are calculated.
	 *  @return The document score.
	 *  @throws IOException Error accessing the Lucene index
	 */
	public double getScore (RetrievalModel r) throws IOException {
		if (! this.docIteratorHasMatchCache()) {
			return this.getDefaultScore(r);
		}
		if (r instanceof RetrievalModelUnrankedBoolean) {
			return this.getScoreUnrankedBoolean (r);
		} else if (r instanceof RetrievalModelRankedBoolean){
			return this.getScoreRankedBoolean(r);
		}
		else if (r instanceof RetrievalModelBM25){
			return this.getScoreBM25(r);
		}else if (r instanceof RetrievalModelIndri){
			return this.getScoreIndri(r);
		} else{
			throw new IllegalArgumentException
			(r.getClass().getName() + " doesn't support the SCORE operator.");
		}
	}
	private double getDefaultScoreRankedBoolean(RetrievalModel r) {
		return 0.0;
	}

	private double getDefaultScoreUnrankedBoolean(RetrievalModel r) {
		return 0.0;
	}
	private double getDefaultScoreBM25(RetrievalModel r) {
		return 0.0;
	}
	
	private double getDefaultScoreIndri(RetrievalModel r) {
		QryIop qry = this.getArg(0);
		return computeIndriScore(0,r,qry);
	}
	
	private double computeIndriScore(int termFrequency, RetrievalModel r,QryIop qry) {

		RetrievalModelIndri model = (RetrievalModelIndri) r;

		int ctf = qry.getCtf();
		long cLen = 0;
		int docLen = 0;
		try {
			cLen = Idx.getSumOfFieldLengths(qry.getField());
			docLen = Idx.getFieldLength(qry.getField(), model.currentDoc);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		double p_q_c = ((double)ctf)/cLen;
		

		return (1-model.lambda)*((termFrequency + model.mu*p_q_c)/(docLen + model.mu)) +
				model.lambda*p_q_c;
	}


	private double getScoreIndri(RetrievalModel r) throws IOException{
		QryIop qry = this.getArg(0);
		
		return computeIndriScore(qry.docIteratorGetMatchPosting().tf,r,qry);
		
	}

	private double getScoreBM25(RetrievalModel r) throws IOException {


		RetrievalModelBM25 model= (RetrievalModelBM25) r;
		QryIop qry = this.getArg(0);


		int tf = qry.docIteratorGetMatchPosting().tf;
		int docLen = Idx.getFieldLength(qry.getField(), qry.docIteratorGetMatchPosting().docid);
		int df = qry.getDf();
		long N = Idx.getNumDocs();
		double qtf = 1.0;
		double averageDocLen = 0.0;

		if(model.containsField(qry.getField())){
			averageDocLen = model.getAverageDocLen(qry.getField());
		}else{
			averageDocLen = Idx.getSumOfFieldLengths(qry.getField()) /
					(float) Idx.getDocCount (qry.getField());
			model.addAverageDocLen(qry.getField(), averageDocLen);
		}

		return Math.max(0, Math.log((N + 0.5 - df)/(df+0.5)))*
				(tf/(tf+model.getK1()*(1-model.getB()+model.getB()*docLen/averageDocLen)))*
				((model.getK3()+1)*qtf/(model.getK3()+qtf));

	}
	/**
	 *  getScore for the Unranked retrieval model.
	 *  @param r The retrieval model that determines how scores are calculated.
	 *  @return The document score.
	 *  @throws IOException Error accessing the Lucene index
	 */
	public double getScoreUnrankedBoolean (RetrievalModel r) throws IOException {

		return 1.0;

	}
	public double getScoreRankedBoolean (RetrievalModel r) throws IOException {

		QryIop qry = this.getArg(0);
		//Return score as term frequency
		return qry.docIteratorGetMatchPosting().tf;

	}

	/**
	 *  Initialize the query operator (and its arguments), including any
	 *  internal iterators.  If the query operator is of type QryIop, it
	 *  is fully evaluated, and the results are stored in an internal
	 *  inverted list that may be accessed via the internal iterator.
	 *  @param r A retrieval model that guides initialization
	 *  @throws IOException Error accessing the Lucene index.
	 */
	public void initialize (RetrievalModel r) throws IOException {
		Qry q = this.args.get (0);
		q.initialize (r);
	}

	@Override
	public double getDefaultScore(RetrievalModel r){
		if (r instanceof RetrievalModelUnrankedBoolean) {
			return this.getDefaultScoreUnrankedBoolean (r);
		} else if( r instanceof RetrievalModelRankedBoolean){
			return this.getDefaultScoreRankedBoolean(r);
		}else if (r instanceof RetrievalModelIndri){
			return this.getDefaultScoreIndri(r);
		}else if (r instanceof RetrievalModelBM25){
			return this.getDefaultScoreBM25(r);
		} else{
			throw new IllegalArgumentException
			(r.getClass().getName() + " doesn't support the AND operator.");
		}
	}

	
	
}
