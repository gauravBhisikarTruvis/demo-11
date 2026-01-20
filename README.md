CREATE OR REPLACE DISTRIBUTED QUERY isolated_nodes(/* Parameters here */)
FOR GRAPH Query_Genie {

  v_isolated_vertex =
    SELECT s
    FROM person:s
    WHERE s.outdegree() == 0;

  PRINT v_isolated_vertex.size();
}
