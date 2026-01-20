INTERPRET QUERY () FOR GRAPH Query_Genie {

  SumAccum<INT> @out_edge_count;
  SumAccum<INT> @in_edge_count;

  start_persons = {person.*};

  // Calculate outgoing edge count for each person
  persons_with_out_edges = SELECT p
    FROM start_persons:p
      -(HAS_USED:e1)-> device:d1
    ACCUM p.@out_edge_count += 1;

  persons_with_out_edges = SELECT p
    FROM start_persons:p
      -(HAS_IP:e2)-> ipaddress:ip1
    ACCUM p.@out_edge_count += 1;

  persons_with_out_edges = SELECT p
    FROM start_persons:p
      -(HAS_ACCOUNT:e3)-> accountnumber:a1
    ACCUM p.@out_edge_count += 1;

  persons_with_out_edges = SELECT p
    FROM start_persons:p
      -(HAS_PHONE:e4)-> phone:ph1
    ACCUM p.@out_edge_count += 1;

  persons_with_out_edges = SELECT p
    FROM start_persons:p
      -(HAS_EMAIL:e5)-> email:em1
    ACCUM p.@out_edge_count += 1;

  // Calculate incoming edge count for each person
  persons_with_in_edges = SELECT p
    FROM start_persons:p
      <-(reverse_HAS_USED:e6)- device:d2
    ACCUM p.@in_edge_count += 1;

  persons_with_in_edges = SELECT p
    FROM start_persons:p
      <-(reverse_HAS_IP:e7)- ipaddress:ip2
    ACCUM p.@in_edge_count += 1;

  persons_with_in_edges = SELECT p
    FROM start_persons:p
      <-(reverse_HAS_ACCOUNT:e8)- accountnumber:a2
    ACCUM p.@in_edge_count += 1;

  persons_with_in_edges = SELECT p
    FROM start_persons:p
      <-(reverse_HAS_PHONE:e9)- phone:ph2
    ACCUM p.@in_edge_count += 1;

  persons_with_in_edges = SELECT p
    FROM start_persons:p
      <-(reverse_HAS_EMAIL:e10)- email:em2
    ACCUM p.@in_edge_count += 1;

  // Filter for persons with no incoming and no outgoing edges
  isolated_persons = SELECT p
    FROM start_persons:p
    WHERE p.@out_edge_count == 0 AND p.@in_edge_count == 0
    LIMIT 100;

  PRINT isolated_persons;
}
