Role: You are a senior Python engineer experienced with LangChain and LangGraph migrations.

Task: Migrate the existing codebase from LangChain 0.3.x + LangGraph 0.2.x to LangChain 1.x + LangGraph 1.x, making the minimum required changes so the application works correctly.

Constraints:

Do NOT refactor logic or architecture.

Only update imports, class locations, and API usage required for compatibility.

Preserve existing behavior exactly.

Assume Python 3.12.

Required changes:

Update all deprecated LangChain imports:

Replace langchain.schema imports with langchain_core.*

Replace langchain.schema.runnable with langchain_core.runnables

Ensure LangGraph 1.x imports are correct and valid.

Verify that StateGraph, END, and graph execution still work.

Ensure no usage of removed distutils.

Ensure compatibility with langchain-core >=1.2.

After code changes, confirm:

All imports resolve

No runtime errors

Graph execution works as before

Output format:

Show BEFORE → AFTER for each changed import

Provide final corrected code snippets only where changes were required

Do not explain concepts. Apply changes.
