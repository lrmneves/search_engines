/*
 *  Copyright (c) 2016, Carnegie Mellon University.  All Rights Reserved.
 *  Version 3.1.1.
 */

import java.io.*;
import java.util.*;

import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 * QryEval is a simple application that reads queries from a file,
 * evaluates them against an index, and writes the results to an
 * output file.  This class contains the main method, a method for
 * reading parameter and query files, initialization methods, a simple
 * query parser, a simple query processor, and methods for reporting
 * results.
 * <p>
 * This software illustrates the architecture for the portion of a
 * search engine that evaluates queries.  It is a guide for class
 * homework assignments, so it emphasizes simplicity over efficiency.
 * Everything could be done more efficiently and elegantly.
 * <p>
 * The {@link Qry} hierarchy implements query evaluation using a
 * 'document at a time' (DaaT) methodology.  Initially it contains an
 * #OR operator for the unranked Boolean retrieval model and a #SYN
 * (synonym) operator for any retrieval model.  It is easily extended
 * to support additional query operators and retrieval models.  See
 * the {@link Qry} class for details.
 * <p>
 * The {@link RetrievalModel} hierarchy stores parameters and
 * information required by different retrieval models.  Retrieval
 * models that need these parameters (e.g., BM25 and Indri) use them
 * very frequently, so the RetrievalModel class emphasizes fast access.
 * <p>
 * The {@link Idx} hierarchy provides access to information in the
 * Lucene index.  It is intended to be simpler than accessing the
 * Lucene index directly.
 * <p>
 * As the search engine becomes more complex, it becomes useful to
 * have a standard approach to representing documents and scores.
 * The {@link ScoreList} class provides this capability.
 */
public class QryEval {

	//  --------------- Constants and variables ---------------------

	private static final String USAGE =
			"Usage:  java QryEval paramFile\n\n";

	private static final EnglishAnalyzerConfigurable ANALYZER =
			new EnglishAnalyzerConfigurable(Version.LUCENE_43);
	private static final String[] TEXT_FIELDS =
		{ "body", "title", "url", "inlink" };
	static StringBuilder builder;
	static PrintWriter outputExQueryFile;
	static HashMap<String,InvList> invListMap = new HashMap<>();
	static Map<String, String> parameters;

	//  --------------- Methods ---------------------------------------

	/**
	 * @param args The only argument is the parameter file name.
	 * @throws Exception Error accessing the Lucene index.
	 */
	public static void main(String[] args) throws Exception {

		//  This is a timer that you may find useful.  It is used here to
		//  time how long the entire program takes, but you can move it
		//  around to time specific parts of your code.

		Timer timer = new Timer();
		timer.start ();

		//  Check that a parameter file is included, and that the required
		//  parameters are present.  Just store the parameters.  They get
		//  processed later during initialization of different system
		//  components.

		if (args.length < 1) {
			throw new IllegalArgumentException (USAGE);
		}

		parameters = readParameterFile (args[0]);

		//  Configure query lexical processing to match index lexical
		//  processing.  Initialize the index and retrieval model.

		ANALYZER.setLowercase(true);
		ANALYZER.setStopwordRemoval(true);
		ANALYZER.setStemmer(EnglishAnalyzerConfigurable.StemmerType.KSTEM);

		Idx.initialize (parameters.get ("indexPath"));
		RetrievalModel model = initializeRetrievalModel (parameters);
		builder = new StringBuilder();
		//  Perform experiments.
		try{
			outputExQueryFile = new PrintWriter(new BufferedWriter(new FileWriter(parameters.get("trecEvalOutputPath"), false)));

		}catch (IOException e) {
			//exception handling left as an exercise for the reader
		}
		processQueryFile(parameters.get("queryFilePath"), model);

		//  Clean up.

		timer.stop ();
		System.out.println ("Time:  " + timer);
		outputExQueryFile.write(builder.toString().substring(0,builder.length()-1));
		outputExQueryFile.close();
	}

	/**
	 * Allocate the retrieval model and initialize it using parameters
	 * from the parameter file.
	 * @return The initialized retrieval model
	 * @throws IOException Error accessing the Lucene index.
	 */
	private static RetrievalModel initializeRetrievalModel (Map<String, String> parameters)
			throws IOException {

		RetrievalModel model = null;
		String modelString = parameters.get ("retrievalAlgorithm").toLowerCase();

		if (modelString.equals("unrankedboolean")) {
			model = new RetrievalModelUnrankedBoolean();
		}else if (modelString.equals("rankedboolean")) {
			model = new RetrievalModelRankedBoolean();
		}else if (modelString.equalsIgnoreCase("bm25")){
			double k_1 = Double.parseDouble(parameters.get("BM25:k_1"));
			double k_3 = Double.parseDouble(parameters.get("BM25:k_3"));
			double b = Double.parseDouble(parameters.get("BM25:b"));
			model = new RetrievalModelBM25(k_1,k_3,b);
		}else if (modelString.equalsIgnoreCase("indri")){
			int mu = Integer.parseInt(parameters.get("Indri:mu"));
			double lambda = Double.parseDouble(parameters.get("Indri:lambda"));
			model = new RetrievalModelIndri(mu,lambda);
		}
		else {
			throw new IllegalArgumentException
			("Unknown retrieval model " + parameters.get("retrievalAlgorithm"));
		}

		return model;
	}

	/**
	 * Optimize the query by removing degenerate nodes produced during
	 * query parsing, for example '#NEAR/1 (of the)' which turns into 
	 * '#NEAR/1 ()' after stopwords are removed; and unnecessary nodes
	 * or subtrees, such as #AND (#AND (a)), which can be replaced by 'a'.
	 */
	static Qry optimizeQuery(Qry q) {

		//  Term operators don't benefit from optimization.

		if (q instanceof QryIopTerm) {
			return q;
		}

		//  Optimization is a depth-first task, so recurse on query
		//  arguments.  This is done in reverse to simplify deleting
		//  query arguments that become null.

		for (int i = q.args.size() - 1; i >= 0; i--) {

			Qry q_i_before = q.args.get(i);
			Qry q_i_after = optimizeQuery (q_i_before);

			if (q_i_after == null) {
				q.removeArg(i);			// optimization deleted the argument
			} else {
				if (q_i_before != q_i_after) {
					q.args.set (i, q_i_after);	// optimization changed the argument
				}
			}
		}

		//  If the operator now has no arguments, it is deleted.

		if (q.args.size () == 0) {
			return null;
		}

		//  Only SCORE operators can have a single argument.  Other
		//  query operators that have just one argument are deleted.

		if ((q.args.size() == 1) &&
				(! (q instanceof QrySopScore))) {
			q = q.args.get (0);
		}

		return q;

	}

	/**
	 * Return a query tree that corresponds to the query.
	 * 
	 * @param qString
	 *          A string containing a query.
	 * @param qTree
	 *          A query tree
	 * @throws IOException Error accessing the Lucene index.
	 */
	static Stack<Qry> opStack;
	static Qry parseQuery(String qString, RetrievalModel model) throws IOException {

		//  Add a default query operator to every query. This is a tiny
		//  bit of inefficiency, but it allows other code to assume
		//  that the query will return document ids and scores.

		String defaultOp = model.defaultQrySopName ();
		qString = defaultOp + "(" + qString + ")";

		//  Simple query tokenization.  Terms like "near-death" are handled later.

		StringTokenizer tokens = new StringTokenizer(qString, "\t\n\r ,()", true);
		String token = null;

		//  This is a simple, stack-based parser.  These variables record
		//  the parser's state.

		Qry currentOp = null;
		opStack = new Stack<Qry>();
		Stack<Qry> weightStack = new Stack<Qry>();

		//  Each pass of the loop processes one token. The query operator
		//  on the top of the opStack is also stored in currentOp to
		//  make the code more readable.

		while (tokens.hasMoreTokens()) {

			token = tokens.nextToken();

			if (token.matches("[ ,(\t\n\r]")) {
				continue;
			} else if (token.equals(")")) {	// Finish current query op.

				// If the current query operator is not an argument to another
				// query operator (i.e., the opStack is empty when the current
				// query operator is removed), we're done (assuming correct
				// syntax - see below).

				opStack.pop();

				if (opStack.empty())
					break;

				// Not done yet.  Add the current operator as an argument to
				// the higher-level operator, and shift processing back to the
				// higher-level operator.

				Qry arg = currentOp;
				currentOp = opStack.peek();
				currentOp.appendArg(arg);

			} else if (token.equalsIgnoreCase("#or")) {
				currentOp = new QrySopOr ();
				currentOp.setDisplayName (token);
				setWeight();
				opStack.push(currentOp);
			}else if (token.equalsIgnoreCase("#and")){
				currentOp = new QrySopAnd ();
				currentOp.setDisplayName (token);
				setWeight();
				opStack.push(currentOp);
			}else if (token.equalsIgnoreCase("#wand")){
				currentOp = new QrySopWAnd ();
				currentOp.setDisplayName (token);
				setWeight();
				opStack.push(currentOp);
				setWeight();
			} else if (token.equalsIgnoreCase("#syn")) {
				currentOp = new QryIopSyn();
				currentOp.setDisplayName (token);
				setWeight();
				opStack.push(currentOp);
			}else if (token.equalsIgnoreCase("#sum")){
				currentOp = new QrySopSum();
				currentOp.setDisplayName (token);
				setWeight();
				opStack.push(currentOp);
			}else if (token.equalsIgnoreCase("#wsum")){
				currentOp = new QrySopWSum ();
				currentOp.setDisplayName (token);
				setWeight();
				opStack.push(currentOp);
				setWeight();
			}else if (token.split("/")[0].equalsIgnoreCase("#near")){
				String [] operation = token.split("/");
				currentOp = new QryIopNear(Integer.parseInt(operation[1]));
				currentOp.setDisplayName (operation[0]);
				setWeight();
				opStack.push(currentOp);
			} else if (token.split("/")[0].equalsIgnoreCase("#window")){
				String [] operation = token.split("/");
				currentOp = new QryIopWindow(Integer.parseInt(operation[1]));
				currentOp.setDisplayName (operation[0]);
				setWeight();
				opStack.push(currentOp);
			} else {

				if(opStack.peek().expectWeight && 
						(isWeightedOp())){
					try
					{
						double w = Double.parseDouble(token);
						QrySop curr = (QrySop) opStack.peek();
						curr.weights.add(w);
						curr.expectWeight = false;
						continue;

					}
					catch(NumberFormatException e)
					{
						e.printStackTrace();
					}
				}
				//  Split the token into a term and a field.

				int delimiter = token.indexOf('.');
				String field = null;
				String term = null;

				if (delimiter < 0) {
					field = "body";
					term = token;
				} else {
					field = token.substring(delimiter + 1).toLowerCase();
					term = token.substring(0, delimiter);
				}

				if ((field.compareTo("url") != 0) &&
						(field.compareTo("keywords") != 0) &&
						(field.compareTo("title") != 0) &&
						(field.compareTo("body") != 0) &&
						(field.compareTo("inlink") != 0)) {
					throw new IllegalArgumentException ("Error: Unknown field " + token);
				}

				//  Lexical processing, stopwords, stemming.  A loop is used
				//  just in case a term (e.g., "near-death") gets tokenized into
				//  multiple terms (e.g., "near" and "death").

				String t[] = tokenizeQuery(term);
				if(t.length == 0 && isWeightedOp()){
					QrySop curr = (QrySop) opStack.peek();
					curr.weights.remove(curr.weights.size()-1);
				}
				setWeight();
				for (int j = 0; j < t.length; j++) {
					if (isWeightedOp() && j > 0){
						QrySop curr = (QrySop) opStack.peek();
						curr.weights.add(curr.weights.get(curr.weights.size()-1));
					}
					Qry termOp = new QryIopTerm(t [j], field);

					currentOp.appendArg (termOp);
				}

				//Creates an InvList for that field being queried
				if(!invListMap.containsKey(field)){
					invListMap.put(field, new InvList(field));
				}

			}
		}

		//  A broken structured query can leave unprocessed tokens on the opStack,

		if (tokens.hasMoreTokens()) {
			throw new IllegalArgumentException
			("Error:  Query syntax is incorrect.  " + qString);
		}

		return currentOp;
	}
	public static void setWeight(){
		if(!opStack.isEmpty() && isWeightedOp()) opStack.peek().expectWeight = true;
	}
	public static boolean isWeightedOp(){
		return (opStack.peek() instanceof QrySopWAnd || opStack.peek() instanceof QrySopWSum);
	}
	/**
	 * Print a message indicating the amount of memory used. The caller
	 * can indicate whether garbage collection should be performed,
	 * which slows the program but reduces memory usage.
	 * 
	 * @param gc
	 *          If true, run the garbage collector before reporting.
	 */
	public static void printMemoryUsage(boolean gc) {

		Runtime runtime = Runtime.getRuntime();

		if (gc)
			runtime.gc();

		System.out.println("Memory used:  "
				+ ((runtime.totalMemory() - runtime.freeMemory()) / (1024L * 1024L)) + " MB");
	}

	/**
	 * Process one query.
	 * @param qString A string that contains a query.
	 * @param model The retrieval model determines how matching and scoring is done.
	 * @return Search results
	 * @throws IOException Error accessing the index
	 */
	static ScoreList processQuery(String qString, RetrievalModel model)
			throws IOException {

		Qry q = parseQuery(qString, model);
		q = optimizeQuery (q);

		// Show the query that is evaluated

		//		System.out.println("    --> " + q);

		if (q != null) {

			ScoreList r = new ScoreList ();

			if (q.args.size () > 0) {		// Ignore empty queries

				q.initialize (model);

				while (q.docIteratorHasMatch (model)) {
					int docid = q.docIteratorGetMatch ();
					if(model instanceof RetrievalModelIndri){
						((RetrievalModelIndri)model).currentDoc = docid;
					}
					double score = ((QrySop) q).getScore (model);
					r.add (docid, score);
					q.docIteratorAdvancePast (docid);
				}
			}

			r.sortExternal();

			return r;
		} else
			return null;
	}

	/**
	 * Process the query file.
	 * @param queryFilePath
	 * @param model
	 * @throws IOException Error accessing the Lucene index.
	 */
	static void processQueryFile(String queryFilePath,RetrievalModel model)
			throws IOException {

		BufferedReader input = null;

		try {
			String qLine = null;

			input = new BufferedReader(new FileReader(queryFilePath));

			//  Each pass of the loop processes one query.

			while ((qLine = input.readLine()) != null) {
				int d = qLine.indexOf(':');

				if (d < 0) {
					throw new IllegalArgumentException
					("Syntax error:  Missing ':' in query line.");
				}

				printMemoryUsage(false);

				String qid = qLine.substring(0, d);
				String query = qLine.substring(d + 1);

				ScoreList r = null;
				//If no query expansion, run hw3 code
				if(!parameters.containsKey("fb") || parameters.get("fb").equals("false")){
					r = processQuery(query, model);
					if (r != null) {
						printResults(qid, r);
					}
				}else{
					String f = "body";
					//else, create an arraylist of top documents from either the original query or the 
					//given files and a map from docid to weight.
					ArrayList<Integer> topDocsList = new ArrayList<>();
					HashMap<String,TopTerm> topTermsMap = new HashMap<>();
					int fbDocs = Integer.parseInt(parameters.get("fbDocs"));
					HashMap<Integer,Double> docScoreMap = new HashMap<>();

					int fbTerms = Integer.parseInt(parameters.get("fbTerms"));
					if(!parameters.containsKey("fbInitialRankingFile")){
						r = processQuery(query, model);
						for(int i = 0; i < fbDocs;i++){
							topDocsList.add(r.getDocid(i));
							docScoreMap.put(r.getDocid(i), r.getDocidScore(i));
							TermVector termVector = new TermVector(r.getDocid(i),f);
							for(int j = 0; j < termVector.stemsLength();j++){
								if(termVector.stemString(j) == null || termVector.stemString(j).contains(".") || 
										termVector.stemString(j).contains(",")){
									continue;
								}
								if(!topTermsMap.containsKey(termVector.stemString(j))) topTermsMap.put(termVector.stemString(j), 
										new TopTerm(termVector.stemString(j),0.0,
												termVector.totalStemFreq(j)));
							}
						}
					}else{

						File fbInitialRankingFile = new File (parameters.get("fbInitialRankingFile"));

						if (! fbInitialRankingFile.canRead ()) {
							throw new IllegalArgumentException
							("Can't read " + fbInitialRankingFile);
						}
						Scanner scan = new Scanner(fbInitialRankingFile);
						String line = null;
						for(int i = 0; i < fbDocs;i++){
							if(scan.hasNextLine()){
								line = scan.nextLine();
								String[] values = line.split (" ");
								int internalId = Idx.getInternalDocid(values[2]);
								topDocsList.add(internalId);
								TermVector termVector = new TermVector(internalId,f);
								for(int j = 0; j < termVector.stemsLength();j++){
									if(termVector.stemString(j) == null || termVector.stemString(j).contains(".") || 
											termVector.stemString(j).contains(",")){
										continue;
									}
									if(!topTermsMap.containsKey(termVector.stemString(j))) topTermsMap.put(termVector.stemString(j), 
											new TopTerm(termVector.stemString(j),0.0,
													termVector.totalStemFreq(j)));
								}
								docScoreMap.put(internalId, 
										Double.parseDouble(values[4]));
							}else{
								break;
							}
						}

						scan.close();

					}
					HashMap<String,Long> fieldLenMap = new HashMap<>();

					int mu = Integer.parseInt(parameters.get("fbMu"));
					for(int doc : topDocsList){
						if(!fieldLenMap.containsKey(f)){
							fieldLenMap.put(f, Idx.getSumOfFieldLengths(f));
						}
						long fieldlen = fieldLenMap.get(f);
						long docFieldLen = Idx.getFieldLength(f, doc);
						TermVector termVector = new TermVector(doc,f);
						for(int i = 0; i < termVector.stemsLength();i++){
							if(!topTermsMap.containsKey(termVector.stemString(i))) {
								continue;
							}

							//For each documents and field, compute score for each term
							//and add to global score

							double tf = (termVector.stemFreq(i) + mu* 
									((double)termVector.totalStemFreq(i))/fieldlen)/(docFieldLen + mu);

							double s = tf * docScoreMap.get(doc)*
									Math.log(fieldlen/((double)termVector.totalStemFreq(i)));
							String term = termVector.stemString(i);

							topTermsMap.get(term).updateScore(s);
						}
						

						for(String t : topTermsMap.keySet()){
							
							double defaultScore = docScoreMap.get(doc)*(mu* 
									((double)topTermsMap.get(t).totalFreq)/fieldlen)/(docFieldLen + mu);
							topTermsMap.get(t).unsetPresent(
									defaultScore*Math.log(fieldlen/
											((double)topTermsMap.get(t).totalFreq)));
						}

					}
					ArrayList<TopTerm> topTermsList = new ArrayList<>();
					topTermsList.addAll(topTermsMap.values());
					Collections.sort(topTermsList);
					//creates expanded query and persist to disk
					if(parameters.containsKey("fbExpansionQueryFile")){
						PrintWriter outputExpansionQueryFile = new PrintWriter(new BufferedWriter(
								new FileWriter(parameters.get("fbExpansionQueryFile"), true)));

						StringBuffer expandedQueryBuffer = new StringBuffer();
						expandedQueryBuffer.append( "#wand (");
						for(int i = 0; i <fbTerms;i++){
							expandedQueryBuffer.append(" " + topTermsList.get(i).score + " " + 
									topTermsList.get(i).term);

						}
						expandedQueryBuffer.append(")");


						double originalWeight = Double.parseDouble(parameters.get("fbOrigWeight"));
						//Add default operator so that we can divide the weight
						String defaultOp = model.defaultQrySopName ();
						query = defaultOp + "(" + query + ")";

						String newQuery = "#wand ( " + originalWeight + " " + query + 
								" " + (1-originalWeight) + " " + expandedQueryBuffer.toString() + " )";
						//Process the new, expanded query
						System.out.println(newQuery);
						r = processQuery(newQuery, model);
						if (r != null) {
							printResults(qid, r);
						}
						//persist expansion part to disk.
						outputExpansionQueryFile.write(qid+": "+ expandedQueryBuffer.toString()+"\n");
						outputExpansionQueryFile.close();
					}
				}



			}
		} catch (IOException ex) {
			ex.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			input.close();
		}
	}

	/**
	 * Print the query results.
	 * 
	 * THIS IS NOT THE CORRECT OUTPUT FORMAT. YOU MUST CHANGE THIS METHOD SO
	 * THAT IT OUTPUTS IN THE FORMAT SPECIFIED IN THE HOMEWORK PAGE, WHICH IS:
	 * 
	 * QueryID Q0 DocID Rank Score RunID
	 * 
	 * @param queryName
	 *          Original query.
	 * @param result
	 *          A list of document ids and scores
	 * @throws IOException Error accessing the Lucene index.
	 */
	static void printResults(String queryName, ScoreList result) throws IOException {

		String exp_id = "fubar";


		if (result.size() < 1) {
			String output = queryName + " Q0 dummy 1 0.000000000000 "+ exp_id;
			System.out.println(output);
			builder.append(output).append("\n");
		} else {
			result.truncate(100);
			for (int i = 0; i < result.size(); i++) {
				String output =queryName + " Q0 " + Idx.getExternalDocid(result.getDocid(i)) 
				+ " " +(i+1) +" " + String.format("%.12f",result.getDocidScore(i)) + " " + exp_id;
				builder.append(output).append("\n");
				System.out.println(output);
			}
		}
	}

	/**
	 * Read the specified parameter file, and confirm that the required
	 * parameters are present.  The parameters are returned in a
	 * HashMap.  The caller (or its minions) are responsible for
	 * processing them.
	 * @return The parameters, in <key, value> format.
	 */
	private static Map<String, String> readParameterFile (String parameterFileName)
			throws IOException {

		Map<String, String> parameters = new HashMap<String, String>();

		File parameterFile = new File (parameterFileName);

		if (! parameterFile.canRead ()) {
			throw new IllegalArgumentException
			("Can't read " + parameterFileName);
		}

		Scanner scan = new Scanner(parameterFile);
		String line = null;
		do {
			line = scan.nextLine();
			String[] pair = line.split ("=");
			if(pair.length == 2) parameters.put(pair[0].trim(), pair[1].trim());
		} while (scan.hasNext());

		scan.close();

		if (! (parameters.containsKey ("indexPath") &&
				parameters.containsKey ("queryFilePath") &&
				parameters.containsKey ("trecEvalOutputPath") &&
				parameters.containsKey ("retrievalAlgorithm"))) {
			throw new IllegalArgumentException
			("Required parameters were missing from the parameter file.");
		}

		return parameters;
	}

	/**
	 * Given a query string, returns the terms one at a time with stopwords
	 * removed and the terms stemmed using the Krovetz stemmer.
	 * 
	 * Use this method to process raw query terms.
	 * 
	 * @param query
	 *          String containing query
	 * @return Array of query tokens
	 * @throws IOException Error accessing the Lucene index.
	 */
	static String[] tokenizeQuery(String query) throws IOException {

		TokenStreamComponents comp =
				ANALYZER.createComponents("dummy", new StringReader(query));
		TokenStream tokenStream = comp.getTokenStream();

		CharTermAttribute charTermAttribute =
				tokenStream.addAttribute(CharTermAttribute.class);
		tokenStream.reset();

		List<String> tokens = new ArrayList<String>();

		while (tokenStream.incrementToken()) {
			String term = charTermAttribute.toString();
			tokens.add(term);
		}

		return tokens.toArray (new String[tokens.size()]);
	}

}
//Class to accumulate scores for each term
class TopTerm implements Comparable<TopTerm>{
	String term;
	double score;
	boolean present;
	long totalFreq;
	int updates = 0;

	TopTerm(String term, double score,long totalFreq){
		this.term = term;
		this.score = score;
		this.totalFreq = totalFreq;
		present = false;
	}
	void updateScore(double update){
		score += update;
		present = true;
		updates++;
	}
	void unsetPresent(double update){
		if(present == true) present = false;
		else{
			score+=update;
			updates++;

		}
	}
	//Max sort, get higher values first.
	@Override
	public int compareTo(TopTerm o) {
		return (o.score - this.score > 0?1: o.score - this.score < 0 ? -1:0);
	}


}