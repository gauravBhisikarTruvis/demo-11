INTERPRET QUERY () FOR GRAPH Query_Genie {

  SumAccum<INT> @@isolated_person_count;
  start_persons = (person.*);

  isolated_persons =
    SELECT p
    FROM start_persons:p
    WHERE p.outdegree() == 0 AND p.indegree() == 0
    ACCUM @@isolated_person_count += 1;

  PRINT @@isolated_person_count;
}


CREATE QUERY isolated_persons() FOR GRAPH Query_Genie {

  SumAccum<INT> @@isolated_person_count;

  isolated_persons =
    SELECT p
    FROM person:p
    WHERE p.indegree() == 0 AND p.outdegree() == 0
    ACCUM @@isolated_person_count += 1;

  PRINT @@isolated_person_count;
}
