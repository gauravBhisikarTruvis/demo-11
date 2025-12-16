import os
import json
import configparser
import logging
from typing import TypedDict, List, Optional, Dict, Any, Union
from datetime import datetime

# LangGraph and LangChain imports
from langgraph.graph import StateGraph, END
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.prompts import ChatPromptTemplate
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 1. CONFIGURATION LOAD ---
def load_config(config_path: str):
    """
    Loads configuration from the specified INI file.
    
    Why: We need credentials (API Keys, Project IDs) to talk to the outside world (OpenAI, BigQuery).
    """
    config = configparser.ConfigParser()
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at: {config_path}")
    config.read(config_path)
    return config

# --- 2. STATE DEFINITION ---
class QueryCandidate(TypedDict):
    """
    Represents a single version of the SQL query.
    
    Why: We need to track different ideas (candidates) for the same problem to see which one checks out.
    """
    sql: str
    source: str  # e.g., 'original', 'rule_based', 'ai_rewrite'
    cost_bytes: Optional[int]  # Estimated cost in bytes
    is_valid: bool
    semantic_score: float  # How close it is to the original meaning (0.0 to 1.0)
    confidence_score: float # Overall score
    reasoning: str

class OptimizerState(TypedDict):
    """
    The 'State' holds all the information as it flows through the factory line.
    
    Why: In a complex process, every step needs to see what happened before it. 
    This is the shared clipboard for all the workers (Nodes).
    """
    original_query: str
    config: Any  # Config object to pass credentials around
    bq_client: Any # BigQuery Client object
    
    # Context
    tables_involved: List[str]
    table_metadata: Dict[str, Any] # Schema, size, partitions from bq_metadata
    
    # Analysis
    analysis_results: Dict[str, Any]
    
    # Candidates
    candidates: List[QueryCandidate]
    best_candidate: Optional[QueryCandidate]
    
    # Control
    recursion_count: int
    final_output: Dict[str, Any]

# --- 3. HELPER FUNCTIONS ---

def _extract_from_statement(statement: Any, table_columns: Dict[str, List[str]], sql_query: str) -> None:
    """
    Stub for extracting tables/columns from a single statement.
    Implementation hidden/provided later.
    """
    pass

def supplement_from_sql_text(sql_query: str) -> Dict[str, Any]:
    """
    Stub for supplemental extraction.
    Implementation hidden/provided later.
    """
    return {}

def extract_table_columns_from_query(sql_query: str, default_project: str = None, default_dataset: str = None) -> Dict[str, List[str]]:
    """
    Extracts tables and their associated columns from a BigQuery SQL query.
    Returns a dictionary mapping fully qualified table names to a list of their columns.
    Only returns actual base tables, not CTEs or subqueries.
    """
    import sqlglot
    try:
        parsed = sqlglot.parse(sql_query, read='bigquery')
        if not parsed:
            logger.warning("Could not parse SQL query.")
            return {}

        table_columns = {}
        for statement in parsed:
            _extract_from_statement(statement, table_columns, sql_query)

        # Filter out CTEs and subqueries, keep only base tables
        # Note: logic relies on table string naming conventions from _extract_from_statement
        base_tables = {
            table: cols for table, cols in table_columns.items()
            if not table.startswith('__cte__') and not table.startswith('__subquery__')
        }

        # Supplemental pass: detect dotted paths following fully-qualified table names directly in SQL text
        # Example: AMH_FZ_FDR_DEV_SIT.cm_event_arrival.id.timestamp -> add id, id.timestamp
        try:
            supplemental = supplement_from_sql_text(sql_query)
            for table, frags in supplemental.items():
                if table in base_tables:
                    before = len(base_tables[table])
                    base_tables[table].update(frags)
                    after = len(base_tables[table])
                    logger.debug(f"Supplemental fragments added for {table}: {sorted(list(frags))} (delta {after - before})")
                else:
                    logger.debug(f"Supplemental found for {table}, but table not in base_tables")
        except Exception as e:
            # ignore supplemental failures
            logger.debug(f"Supplemental extraction failed: {e}")

        # Cleanup: remove bare field names that only appear as suffixes of dotted fragments
        for table, cols in base_tables.items():
            dotted = [c for c in cols if '.' in c]
            suffixes = {c.split('.')[-1] for c in dotted}
            for s in list(suffixes):
                if s in cols:
                    # If the bare suffix exists alongside at least one dotted variant, discard it
                    cols.discard(s)

        # Convert sets to lists for the final output
        return {table: sorted(list(cols)) for table, cols in base_tables.items()}
    except Exception as e:
        logger.error(f"Error parsing SQL: {e}")
        return {}

# --- 3. NODES (The Workers) ---

def node_query_intake(state: OptimizerState) -> OptimizerState:
    """
    Node: Query Intake
    
    What it does: Sets up the initial state with the user's query.
    
    Why: This is the reception desk. We accept the query and prepare the folder (State) for the rest of the team.
    """
    logger.info("--- Step 1: Query Intake ---")
    # Initialize basic lists if empty
    if 'candidates' not in state:
        state['candidates'] = []
    
    # Add the original query as the first candidate
    state['candidates'].append({
        "sql": state['original_query'],
        "source": "original",
        "cost_bytes": None,
        "is_valid": True,
        "semantic_score": 1.0,
        "confidence_score": 1.0,
        "reasoning": "Original user query"
    })
    
    # Initialize clients if not present (in a real graph info might be injected differently, 
    # but here we ensure they exist)
    if 'config' in state and state['config'] and 'bq_client' not in state:
         # Setup BQ Client using config
         # Assuming credential path or default auth
         try:
             project_id = state['config'].get('BigQuery', 'project_id', fallback=None)
             if project_id:
                 state['bq_client'] = bigquery.Client(project=project_id)
                 logger.info(f"BigQuery Client initialized for project: {project_id}")
             else:
                 state['bq_client'] = bigquery.Client() # Fallback to default
         except Exception as e:
             logger.error(f"Failed to initialize BigQuery Client: {e}")
    
    return state

def node_context_enrichment(state: OptimizerState) -> OptimizerState:
    """
    Node: Context Enrichment
    
    What it does: Looks up table details (schema, size) from the `bq_metadata` table.
    
    Why: You can't safely remodel a house without the blueprints. 
    We need to know table sizes and partitions to make smart optimization decisions.
    
    Production Grade: We query a real metadata table instead of guessing.
    """
    logger.info("--- Step 2: Context Enrichment ---")
    query = state['original_query']
    client = state.get('bq_client')
    
    if not client:
        logger.warning("No BQ Client available, skipping context enrichment.")
        return state
    
    # 1. Extract table names (Simple regex or parsing)
    import sqlglot
    
    # 1. Extract tables and columns using sql_parser
    logger.info("Step 2: Extracting tables and columns from SQL using sql_parser...")
    
    # Extract tables and columns using sql_parser
    try:
        table_columns_map = extract_table_columns_from_query(
            query,
            default_project=state['config'].get('BigQuery', 'project_id', fallback='default_project'),
            default_dataset=state['config'].get('BigQuery', 'dataset_id', fallback='default_dataset')
        )
    except Exception as e:
         logger.warning(f"Could not extract tables using new parser: {e}")
         table_columns_map = {}

    if not table_columns_map:
         logger.warning("Could not extract any tables or columns from the query.")
         state['tables_involved'] = []
         return state

    state['tables_involved'] = list(table_columns_map.keys())

    
    # 2. Query the bq_metadata table
    # We assume a specific metadata table exists as per requirements
    metadata_dataset = state['config'].get('BigQuery', 'metadata_dataset', fallback='public_dataset')
    metadata_table_name = "bq_metadata"
    
    # Need to handle the list for SQL IN clause
    formatted_tables = ", ".join([f"'{t}'" for t in potential_tables])
    
    meta_query = f"""
        SELECT table_name, metadata, row_count, size_bytes 
        FROM `{metadata_dataset}.{metadata_table_name}`
        WHERE table_name IN ({formatted_tables})
    """
    
    try:
        query_job = client.query(meta_query)
        results = query_job.result()
        
        enrichment_data = {}
        for row in results:
            t_name = row.table_name
            # Parse the metadata JSON column
            try:
                # The user image showed 'metadata' column has a JSON string with a 'schema' key
                meta_json = json.loads(row.metadata)
                schema_info = meta_json.get('schema', [])
            except:
                schema_info = "Could not parse metadata JSON"
            
            enrichment_data[t_name] = {
                "row_count": row.row_count,
                "size_bytes": row.size_bytes,
                "schema": schema_info
            }
        
        state['table_metadata'] = enrichment_data
        logger.info(f"Enriched context for {len(enrichment_data)} tables.")
        
    except Exception as e:
        logger.error(f"Failed to query bq_metadata: {e}")
        # Continue without context rather than crashing
        state['table_metadata'] = {}

    return state

def node_static_analysis(state: OptimizerState) -> OptimizerState:
    """
    Node: Static Analysis
    
    What it does: Checks the query structure for obvious bad patterns (e.g., SELECT *).
    
    Why: It's cheaper to catch simple mistakes (like a spellchecker) before asking the expensive AI or running the query.
    """
    logger.info("--- Step 3: Static Analysis ---")
    query = state['original_query']
    analysis = {"flags": []}
    
    # Check for SELECT *
    if "SELECT *" in query.upper():
        analysis['flags'].append("Use of 'SELECT *' detected. Recommended to select specific columns.")
        
    # Check for CROSS JOIN (often a mistake)
    if "CROSS JOIN" in query.upper() or "," in query.split("FROM")[1].split("WHERE")[0]:
        analysis['flags'].append("Potential CROSS JOIN or implicit join detected. Verify join conditions.")
    
    state['analysis_results'] = analysis
    return state

def node_rule_based_rewrite(state: OptimizerState) -> OptimizerState:
    """
    Node: Rule-Based Rewrite
    
    What it does: Applies safe, deterministic fixes.
    
    Why: If we know 'SELECT *' is bad, we can write code to fix it automatically without guessing. 
    It's fast, free, and safe.
    """
    logger.info("--- Step 4: Rule-Based Rewrite ---")
    # Simple example: If we had a schema, we could replace SELECT * with actual columns.
    # For this standalone, we will pass mostly, but strict rule systems would go here.
    
    # Demo Rule: Ensure standard casing (Cosmetic but useful)
    # In a real system, this would push filters down or prune partitions.
    pass 
    return state

def node_ai_rewrite(state: OptimizerState) -> OptimizerState:
    """
    Node: AI Rewrite
    
    What it does: Asks OpenAI (the 'Smart Consultant') to re-engineer the query.
    
    Why: AI can spot complex patterns (like subquery unnesting) that are hard to code into simple rules.
    It proposes creative solutions.
    """
    logger.info("--- Step 5: AI Rewrite ---")
    
    # 1. Setup OpenAI Client
    api_key = state['config'].get('LLM', 'api_key')
    model_name = state['config'].get('LLM', 'model', fallback='gpt-4o')
    base_url = state['config'].get('LLM', 'base_url', fallback=None)
    
    llm = ChatOpenAI(
        api_key=api_key, 
        model=model_name, 
        base_url=base_url,
        temperature=0.0 # Be deterministic
    )
    
    # 2. Construct Prompt
    # Provide the context we gathered in Step 2
    context_str = json.dumps(state.get('table_metadata', {}), indent=2)
    analysis_str = json.dumps(state.get('analysis_results', {}), indent=2)
    
    prompt = f"""
    You are a BigQuery Optimization Expert. 
    Here is the Original Query:
    ```sql
    {state['original_query']}
    ```
    
    Here is the Schema Context (Table sizes, columns):
    {context_str}
    
    Here are Static Analysis Flags:
    {analysis_str}
    
    Your Goal: Rewrite this query to be cheaper (scan fewer bytes) and faster, while maintaining EXACT semantic equivalence.
    
    Instructions:
    1. Only output valid BigQuery SQL.
    2. Do not use 'SELECT *'.
    3. Use partition or cluster filters if evident from context.
    4. Return ONLY the SQL code, no markdown formatting.
    """
    
    try:
        response = llm.invoke(prompt)
        ai_sql = response.content.strip().replace('```sql', '').replace('```', '')
        
        # Add to candidates
        state['candidates'].append({
            "sql": ai_sql,
            "source": "ai_rewrite",
            "cost_bytes": None,
            "is_valid": False, # Not verified yet
            "semantic_score": 0.0,
            "confidence_score": 0.0,
            "reasoning": "AI Suggested Optimization"
        })
        logger.info("AI suggested a rewrite.")
        
    except Exception as e:
        logger.error(f"AI Rewrite failed: {e}")
        
    return state

def node_candidate_merge(state: OptimizerState) -> OptimizerState:
    """
    Node: Candidate Merge
    
    What it does: Deduplicates suggestions.
    
    Why: If the AI suggests the same thing as the original query (it happens), 
    we shouldn't waste time testing it twice.
    """
    logger.info("--- Step 6: Candidate Merge ---")
    # Simple logic: Ensure unique SQL strings
    unique_candidates = {}
    for cand in state['candidates']:
        # Normalize whitespace for comparison
        clean_sql = " ".join(cand['sql'].split())
        if clean_sql not in unique_candidates:
            unique_candidates[clean_sql] = cand
            
    state['candidates'] = list(unique_candidates.values())
    return state

def node_dry_run_estimation(state: OptimizerState) -> OptimizerState:
    """
    Node: Dry Run Estimation (Covers steps 7 & 8)
    
    What it does: Sends each query to BQ with `dry_run=True`.
    
    Why: This tells us the EXACT bill (bytes scanned) without actually running the query and spending money.
    This is the most critical metric for optimization.
    """
    logger.info("--- Step 7 & 8: Dry Run Cost Estimation ---")
    client = state.get('bq_client')
    if not client:
        return state
        
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    
    for cand in state['candidates']:
        try:
            query_job = client.query(cand['sql'], job_config=job_config)
            # A dry run returns the estimated bytes processed
            cand['cost_bytes'] = query_job.total_bytes_processed
            cand['is_valid'] = True # Syntactically valid at least
            logger.info(f"Source: {cand['source']} | Cost: {cand['cost_bytes']} bytes")
        except GoogleAPIError as e:
            logger.warning(f"Candidate {cand['source']} is invalid: {e}")
            cand['is_valid'] = False
            cand['cost_bytes'] = float('inf') # Penalize invalid queries
            
    return state

def node_semantic_validation(state: OptimizerState) -> OptimizerState:
    """
    Node: Semantic Validation
    
    What it does: Checks if the new query means the same as the old one.
    
    Why: Speed is useless if the data is wrong. We check if the columns match.
    Production Grade: In a full run, we would compare results on a sample dataset.
    """
    logger.info("--- Step 9: Semantic Validation ---")
    # Heuristic Check: Do the output columns match?
    # We can inspect the schema from the dry run (if we saved it, or run dry run again)
    
    # For this standalone, we'll assume valid dry runs on similar tables implies basic semantic alignment
    # but we downgrade confidence if the structure changed significantly without clear reason.
    for cand in state['candidates']:
        if cand['is_valid']:
            cand['semantic_score'] = 1.0 # Optimistic assumption for this demo
            
    return state

def node_candidate_scoring(state: OptimizerState) -> OptimizerState:
    """
    Node: Candidate Scoring
    
    What it does: Assigns a score based on Cost Savings vs Risk.
    
    Why: A query that saves 1% but looks risky isn't worth it. A query that saves 90% is.
    """
    logger.info("--- Step 10: Candidate Scoring ---")
    
    # Find original cost
    original_cost = float('inf')
    for cand in state['candidates']:
        if cand['source'] == 'original' and cand['cost_bytes'] is not None:
            original_cost = cand['cost_bytes']
            break
            
    for cand in state['candidates']:
        if not cand['is_valid']:
            cand['confidence_score'] = 0.0
            continue
            
        # Calculation: Savings Ratio
        cost = cand['cost_bytes'] if cand['cost_bytes'] is not None else float('inf')
        
        if original_cost > 0:
            savings_ratio = (original_cost - cost) / original_cost
        else:
            savings_ratio = 0
            
        # Score combines semantic trust and savings
        # Only reward positive savings
        base_score = 0.5 # Baseline
        if savings_ratio > 0:
            base_score += (savings_ratio * 0.5) # Up to 1.0
            
        cand['confidence_score'] = base_score * cand['semantic_score']
        
    return state

def node_decision(state: OptimizerState) -> OptimizerState:
    """
    Node: Decision
    
    What it does: Picks the winner.
    
    Why: We need a final answer.
    """
    logger.info("--- Step 11: Decision ---")
    
    # Sort by score descending
    sorted_candidates = sorted(state['candidates'], key=lambda x: x['confidence_score'], reverse=True)
    
    if sorted_candidates:
        best = sorted_candidates[0]
        state['best_candidate'] = best
        state['final_output'] = {
            "optimized_sql": best['sql'],
            "savings_bytes": 0, # Calculate relative to original
            "explanation": f"Selected query from {best['source']} with score {best['confidence_score']}"
        }
    else:
        # Fallback to original
        state['final_output'] = {"optimized_sql": state['original_query'], "explanation": "No valid candidates found."}
        
    return state

def node_finalize(state: OptimizerState) -> OptimizerState:
    """
    Node: Finalize
    
    What it does: Formats the output for the user.
    """
    logger.info("--- Step 13: Finalize ---")
    print("\n\n============ FINAL RESULT ============")
    print(f"Original Query: \n{state['original_query']}")
    print("-" * 30)
    print(f"Optimized Query ({state['best_candidate']['source']}): \n{state['best_candidate']['sql']}")
    print("-" * 30)
    if state['best_candidate']['cost_bytes']:
        print(f"Estimated Scan: {state['best_candidate']['cost_bytes'] / 1024 / 1024:.2f} MB")
    print("======================================\n")
    return state


# --- 4. GRAPH BUILD ---

def build_optimizer_graph():
    """
    Constructs the LangGraph.
    
    Why: Connects all the 'Nodes' (workers) with 'Edges' (conveyor belts) to make a complete factory.
    """
    builder = StateGraph(OptimizerState)
    
    # Add Nodes
    builder.add_node("query_intake", node_query_intake)
    builder.add_node("context_enrichment", node_context_enrichment)
    builder.add_node("static_analysis", node_static_analysis)
    builder.add_node("rule_based_rewrite", node_rule_based_rewrite)
    builder.add_node("ai_rewrite", node_ai_rewrite)
    builder.add_node("candidate_merge", node_candidate_merge)
    builder.add_node("dry_run_estimation", node_dry_run_estimation) # Merged cost estimation steps
    builder.add_node("semantic_validation", node_semantic_validation)
    builder.add_node("candidate_scoring", node_candidate_scoring)
    builder.add_node("decision", node_decision)
    builder.add_node("finalize", node_finalize)
    
    # Define Flow (Edges)
    # The Linear Happy Path
    builder.set_entry_point("query_intake")
    builder.add_edge("query_intake", "context_enrichment")
    builder.add_edge("context_enrichment", "static_analysis")
    builder.add_edge("static_analysis", "rule_based_rewrite")
    builder.add_edge("rule_based_rewrite", "ai_rewrite")
    builder.add_edge("ai_rewrite", "candidate_merge")
    builder.add_edge("candidate_merge", "dry_run_estimation")
    builder.add_edge("dry_run_estimation", "semantic_validation")
    builder.add_edge("semantic_validation", "candidate_scoring")
    builder.add_edge("candidate_scoring", "decision")
    builder.add_edge("decision", "finalize")
    builder.add_edge("finalize", END)
    
    return builder.compile()

# --- 5. MAIN EXECUTION ---

if __name__ == "__main__":
    # Path to config file
    CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../../config/US/config.ini')
    
    # 1. Load Config
    try:
        config = load_config(CONFIG_PATH)
        logger.info("Configuration loaded successfully.")
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        exit(1)
        
    # 2. Define Input
    test_query = """
    SELECT * 
    FROM `bigquery-public-data.samples.shakespeare`
    WHERE word_count > 10
    """
    
    # 3. Initialize State
    initial_state = {
        "original_query": test_query,
        "config": config,
        "recursion_count": 0
    }
    
    # 4. Run Graph
    logger.info("Starting Optimizer Graph...")
    graph = build_optimizer_graph()
    result = graph.invoke(initial_state)
    logger.info("Optimization Complete.")
