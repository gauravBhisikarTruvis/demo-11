INTERPRET QUERY () FOR GRAPH Query_Genie {

  result =
    SELECT p
    FROM person:p
    WHERE p.outdegree("HAS_ACCOUNT") == 0
    LIMIT 100;

  PRINT result;
}
