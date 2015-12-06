#include <vector>
#include <string>
#include <fstream>

#include <graphlab.hpp>

// The vertex data is just the pagerank value (a double)
typedef double vertex_data_type;

// There is no edge data in the pagerank application
typedef graphlab::empty edge_data_type;

// The graph type is determined by the vertex and edge data types
typedef graphlab::distributed_graph<vertex_data_type, edge_data_type> graph_type;
double dangle_contrib = 0.0;

double map_func(const graph_type::vertex_type& vertex){
	int out_num = vertex.num_out_edges();
	if(out_num == 0){
		return vertex.data();
	}else {
		return 0;
	}
}
/* Implement the gather_edges, gather and apply functions in this class 
 */
class pagerank :
  public graphlab::ivertex_program<graph_type, double>,
  public graphlab::IS_POD_TYPE {

public:

  edge_dir_type gather_edges(icontext_type& context,
                              const vertex_type& vertex) const {
		        return graphlab::IN_EDGES;
  }


  double gather(icontext_type& context, const vertex_type& vertex,
               edge_type& edge) const {
			return edge.source().data() / edge.source().num_out_edges();

  }

  void apply(icontext_type& context, vertex_type& vertex,
             const gather_type& total) {
		 double newval = (total + dangle_contrib/2546953.0) * 0.85 + 0.15;
		 vertex.data() = newval;
  }

  // We don't need scatter in the PageRank application
  edge_dir_type scatter_edges(icontext_type& context,
                              const vertex_type& vertex) const {
	return graphlab::NO_EDGES;
  }
};


/*
 * Save the result. Output <id>\t<pagerank>
 */
struct pagerank_writer {
  std::string save_vertex(graph_type::vertex_type v) {
    std::stringstream strm;
    strm << v.id() << "\t" << v.data() << "\n";
    return strm.str();
  }
  std::string save_edge(graph_type::edge_type e) { return ""; }
}; // end of pagerank writer

/* Initial value is 1.0 */
void init_vertex(graph_type::vertex_type& vertex) { 
	vertex.data() = 1;
}

/* You may want to implement more functions here to deal with dangling nodes
 */

int main(int argc, char** argv) {
  // Initialize control plain using mpi
  graphlab::mpi_tools::init(argc, argv);
  graphlab::distributed_control dc;
  global_logger().set_log_level(LOG_INFO);

  // Parse command line options -----------------------------------------------
  graphlab::command_line_options clopts("PageRank algorithm.");
  std::string graph_dir;
  std::string format = "snap";
  std::string exec_type = "synchronous";
  std::string saveprefix;
  int num_iteration;
  clopts.attach_option("graph", graph_dir,
                       "The graph file.  If none is provided "
                       "then a toy graph will be created");
  clopts.attach_option("engine", exec_type,
                       "The engine type synchronous or asynchronous");
  clopts.attach_option("format", format,
                       "The graph file format");
  clopts.attach_option("iterations", num_iteration,
                       "Number of iteration");
  clopts.attach_option("saveprefix", saveprefix,
                       "If set, will save the resultant pagerank to a "
                       "sequence of files with prefix saveprefix");

  if(!clopts.parse(argc, argv)) {
    dc.cout() << "Error in parsing command line arguments." << std::endl;
    return EXIT_FAILURE;
  }

  // Build the graph ----------------------------------------------------------
  graph_type graph(dc, clopts);
  dc.cout() << "Loading graph in format: "<< format << std::endl;
  graph.load_format(graph_dir, format);
  graph.finalize();


  // Initialize the vertex data
  graph.transform_vertices(init_vertex);

  // Start iterating
  graphlab::omni_engine<pagerank> engine(dc, graph, exec_type, clopts);
  for (int i = 0; i < num_iteration; ++i) {
		dangle_contrib = graph.map_reduce_vertices<double>(map_func);
		engine.signal_all();
		engine.start();
  }

  const double runtime = engine.elapsed_seconds();
  dc.cout() << "Finished Running engine in " << runtime
            << " seconds." << std::endl;

  // Save the final graph -----------------------------------------------------
  if (saveprefix != "") {
    graph.save(saveprefix, 
			   pagerank_writer(),
               false,    // do not gzip
               true,     // save vertices
               false, // do not save edges
			   1);   // 1 file per machine
  }

  // Tear-down communication layer and quit -----------------------------------
  graphlab::mpi_tools::finalize();
  return EXIT_SUCCESS;
} // End of main
