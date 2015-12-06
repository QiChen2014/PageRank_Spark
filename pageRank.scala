val  textFile = sc.textFile("s3n://f15-p42/twitter-graph.txt");
val distinctLine = textFile.distinct();

/* for each user, get the list of users that he follows */
val follows = distinctLine.map{line => val t = line.split(" "); (t(0), t(1))};
val aggregate_follows = follows.groupByKey();

/* get all unique ids of all users */
val all_id = distinctLine.flatMap(line => line.split(" ")).distinct();
val id_count = 2546953.0;

/* generate an initial RDD of page rank */
var rank = all_id.map(id => (id, 1.0));

/*
* Join the rank rdd with the following links rdd, 
* Use leftOuterJoin to avoid losing dangling users 
*/
var all_info = rank.leftOuterJoin(aggregate_follows);

var loop = 0;

while(loop<10){

	/* use accumulator the record the ranks contributed from all dangling users */
        val dangle_contrib = sc.accumulator(0.0);

        rank = all_info.flatMap{ line =>
                val vert = line._1;
                val old_rank = line._2._1;
                val IFollow = line._2._2;
                if( IFollow == None){
                    dangle_contrib += old_rank.toDouble;  // if find dangling user, add accumulator
                }
                if ( IFollow != None){
                        val followees = IFollow.get;
                        followees.map( word =>  (word, old_rank/followees.size));
                 }else {
                      List()
                 }
              }
        rank.count();
        val dangle_val = dangle_contrib.value/id_count;

	/*
	 * generate RDD inorder to all ranks from dangling users, 
	 * and to avoid losing users who are followed by nobody
	 */
        val extra_contrib = all_info.keys.map(id => (id, dangle_val))
        rank = rank.union(extra_contrib)
        rank = rank.reduceByKey(_ + _).map( line => (line._1, 0.15 + 0.85*line._2));
	all_info = rank.leftOuterJoin(aggregate_follows);
        loop = loop + 1;
}
val output = rank.map(line => line._1 + '\t' + line._2);
output.saveAsTextFile("s3n://15619pp42/task3_result")
