
val  textFile = sc.textFile("s3n://f15-p42/twitter-graph.txt");
val distinctLine = textFile.distinct();
//val follows_count = distinctLine.flatMap{ line => val t = line.split(" "); Array(   (t(0), 1),  (t(1), 0))};
//val count = follows_count.reduceByKey(_ + _);

val follows = distinctLine.map{line => val t = line.split(" "); (t(0), t(1))};
val aggregate_follows = follows.groupByKey();

//val follows_detail = count.join(aggregate_follows);
//follows_detail.cache();

val all_id = distinctLine.flatMap(line => line.split(" ")).distinct();
//all_id.cache();
val id_count = 2546953.0;
//val id_array = all_id.collect();
var rank = all_id.map(id => (id, 1.0));

//var all_info = rank.join(follows_detail)
var all_info = rank.leftOuterJoin(aggregate_follows);
//var dangle = HashSet[String]();

//val all_info_array= all_info.collect()
//all_info_array.foreach{ line => if(line._2._2 == None) dangle += line._1}

var loop = 0;

while(loop<10){
        val dangle_contrib = sc.accumulator(0.0);

        rank = all_info.flatMap{ line =>
                val vert = line._1;
                val old_rank = line._2._1;
                val IFollow = line._2._2;
                //val count = IFollow.size();
                if( IFollow == None){
                    dangle_contrib += old_rank.toDouble;
                }
                if ( IFollow != None){
                        val followees = IFollow.get;
                        followees.map( word =>  (word, old_rank/followees.size));
                        //followees.foreach{ word => if(dangle.contains(word)) dangle_contrib += old_rank}
                 }else {
                      List()
                 }
              }
        rank.count();
        val dangle_val = dangle_contrib.value/id_count;
        val extra_contrib = all_info.keys.map(id => (id, dangle_val))
        rank = rank.union(extra_contrib)
        rank = rank.reduceByKey(_ + _).map( line => (line._1, 0.15 + 0.85*line._2));
	all_info = rank.leftOuterJoin(aggregate_follows);
        loop = loop + 1;
}
val output = rank.map(line => line._1 + '\t' + line._2);
output.saveAsTextFile("s3n://15619pp42/task3_result")
//rank.saveAsTextFile("hdfs:///output2/")
