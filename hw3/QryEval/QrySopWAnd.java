import java.io.IOException;
import java.util.ArrayList;

public class QrySopWAnd extends QrySop {
	
	Double norm = null;
	
	@Override
	public double getScore(RetrievalModel r) throws IOException {
		if (norm == null){
			norm = 0.0;
			for (double w : this.weights){
				norm+=w;
			}
		}
		if (this.docIteratorHasMatch(r)){
			if (r instanceof RetrievalModelUnrankedBoolean) {
				return this.getScoreUnrankedBoolean (r);
			} else if( r instanceof RetrievalModelRankedBoolean){
				return this.getScoreRankedBoolean(r);
			}else if (r instanceof RetrievalModelIndri){
				return this.getScoreIndri(r);
			}
			else{
				throw new IllegalArgumentException
				(r.getClass().getName() + " doesn't support the AND operator.");
			}
		}else{
			return this.getDefaultScore(r);
		}
	}
	public double getDefaultScore(RetrievalModel r) throws IOException{
		if (r instanceof RetrievalModelUnrankedBoolean) {
			return this.getDefaultScoreUnrankedBoolean (r);
		} else if( r instanceof RetrievalModelRankedBoolean){
			return this.getDefaultScoreRankedBoolean(r);
		}else if (r instanceof RetrievalModelIndri){
			return this.getDefaultScoreIndri(r);
		}
		else{
			throw new IllegalArgumentException
			(r.getClass().getName() + " doesn't support the AND operator.");
		}
	}
	

	private double getDefaultScoreRankedBoolean(RetrievalModel r) {
		return 0.0;
	}

	private double getDefaultScoreUnrankedBoolean(RetrievalModel r) {
		return 0.0;
	}

	private double getDefaultScoreIndri(RetrievalModel r) throws IOException {
		return getScoreIndri(r);	
	}
	
	private double getScoreIndri(RetrievalModel r) throws IOException {

		double prod = 1.0;
		
		int index = 0;
		for(Qry arg : this.args){
			
			if(arg instanceof QrySop){
				QrySop sopArg = (QrySop) arg;
				double score = 0.0;
				if(arg.docIteratorHasMatch(r) &&
						arg.docIteratorGetMatch() == ((RetrievalModelIndri) r).currentDoc ){
					score = sopArg.getScore(r);
				}else{
					score = sopArg.getDefaultScore(r);
				}
				prod*= Math.pow(score, this.weights.get(index)/norm);
			}
			index+=1;
		}

		return prod;
	}


	private double getScoreUnrankedBoolean(RetrievalModel r) {
		return 1.0;
	}
	private double getScoreRankedBoolean(RetrievalModel r) throws IOException {
		//Return the min value of the matches
		double min = Double.MAX_VALUE;
		for(Qry arg : this.args){
			if(!arg.docIteratorHasMatch(r)) continue;

			if(this.docIteratorGetMatch() == arg.docIteratorGetMatch()){
				if(arg instanceof QrySop){
					QrySop sopArg = (QrySop) arg;
					double score = sopArg.getScore(r);
					if(min > score){
						min = score;
					}
				}
			}
		}
		if(min == Double.MAX_VALUE) min = 0.0;
		return min;

	}

	@Override
	public boolean docIteratorHasMatch(RetrievalModel r) {
		if(r instanceof RetrievalModelIndri){
			return this.docIteratorHasMatchMin(r);
		}else{
			return this.docIteratorHasMatchAll(r);
		}
	}

}
