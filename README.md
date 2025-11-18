// At top of component
const [sampleQueries, setSampleQueries] = useState([]); 
const [newQueryValue, setNewQueryValue] = useState('');
const [newQueryDescription, setNewQueryDescription] = useState('');


// after fetching initial data
const loadedSampleUsage = (loadedData.sample_usage && Array.isArray(loadedData.sample_usage))
  ? loadedData.sample_usage.map((item, idx) => ({
      id: item.id || `s-${idx}`,    // keep unique id for UI
      query: item.sql || '',
      description: item.description || '',
      tempQuery: item.sql || '',       // editable temporary values
      tempDescription: item.description || '',
      isEditing: false,
    }))
  : [];

setSampleQueries(loadedSampleUsage);




<table className="sample-usage-table">
  <thead>
    <tr>
      <th>Sample Usage Query</th>
      <th>Description</th>
      <th>Action</th>
    </tr>
  </thead>
  <tbody>
    {sampleQueries.map((q) => (
      <tr key={q.id}>
        <td>
          {q.isEditing ? (
            <textarea
              value={q.tempQuery}
              onChange={(e) => handleTempChange(q.id, 'tempQuery', e.target.value)}
              className="editable-query"
              rows={3}
            />
          ) : (
            <pre style={{whiteSpace: 'pre-wrap', margin: 0}}>{q.query}</pre>
          )}
        </td>

        <td>
          {q.isEditing ? (
            <input
              type="text"
              value={q.tempDescription}
              onChange={(e) => handleTempChange(q.id, 'tempDescription', e.target.value)}
              className="editable-description"
            />
          ) : (
            <div style={{whiteSpace: 'pre-wrap'}}>{q.description || <em>No description</em>}</div>
          )}
        </td>

        <td>
          {q.isEditing ? (
            <>
              <button onClick={() => handleSaveQuery(q.id)}>Save</button>
              <button onClick={() => handleCancelEdit(q.id)}>Cancel</button>
            </>
          ) : (
            <>
              <button onClick={() => handleEdit(q.id)}>Edit</button>
              <button onClick={() => handleDeleteQuery(q.id)}>Delete</button>
            </>
          )}
        </td>
      </tr>
    ))}

    {/* Add new row */}
    <tr>
      <td colSpan="2">
        <input
          type="text"
          placeholder="Enter new query..."
          value={newQueryValue}
          onChange={(e) => setNewQueryValue(e.target.value)}
          style={{width:'100%'}}
        />
        <input
          type="text"
          placeholder="Enter description..."
          value={newQueryDescription}
          onChange={(e) => setNewQueryDescription(e.target.value)}
          style={{width:'100%', marginTop:6}}
        />
      </td>
      <td>
        <button onClick={handleAddNewQuery} disabled={!newQueryValue.trim()}>Save</button>
      </td>
    </tr>
  </tbody>
</table>






const handleTempChange = (id, field, value) => {
  setSampleQueries(prev =>
    prev.map(item => (item.id === id ? {...item, [field]: value} : item))
  );
};

const handleEdit = (id) => {
  setSampleQueries(prev =>
    prev.map(item => item.id === id ? {...item, isEditing: true} : item)
  );
};

const handleCancelEdit = (id) => {
  setSampleQueries(prev =>
    prev.map(item => item.id === id ? {
      ...item,
      isEditing: false,
      tempQuery: item.query,
      tempDescription: item.description
    } : item)
  );
};

const handleSaveQuery = (id) => {
  setSampleQueries(prev =>
    prev.map(item => {
      if (item.id !== id) return item;
      // commit temp values to main fields
      return {
        ...item,
        isEditing: false,
        query: item.tempQuery ?? item.query,
        description: item.tempDescription ?? item.description,
      };
    })
  );
  // optionally auto-call saveToServer() here or wait for main "Save metadata" button
};

const handleDeleteQuery = (id) => {
  setSampleQueries(prev => prev.filter(item => item.id !== id));
};

const handleAddNewQuery = () => {
  const trimmedQuery = newQueryValue.trim();
  if (!trimmedQuery) return;

  const newItem = {
    id: `new-${Date.now()}`, // simple unique id
    query: trimmedQuery,
    description: newQueryDescription || '',
    tempQuery: trimmedQuery,
    tempDescription: newQueryDescription || '',
    isEditing: false,
  };
  setSampleQueries(prev => [newItem, ...prev]);
  setNewQueryValue('');
  setNewQueryDescription('');
};







build payload to save to db

const saveMetadataToServer = async () => {
  try {
    // Build sample_usage array in required format
    const sample_usage = sampleQueries.map(item => ({
      sql: (item.query ?? '').trim(),
      description: (item.description ?? '').trim()
    }));

    // Build payload — adapt other fields as your API expects
    const payload = {
      // include whatever fields your update endpoint needs
      // e.g. id_key, data_source_id, table_name, etc.
      id_key: initialData?.id_key,
      data_source_id: initialData?.data_source_id,
      table_name: initialData?.table_name,
      display_name: initialData?.display_name,
      // sample_usage should be stored as JSON array on backend; either
      // send it as an array (recommended) or JSON.stringify(sample_usage)
      sample_usage: sample_usage,
      // other metadata fields...
    };

    const response = await makeApiRequest('/update-table-metadata', {
      body: JSON.stringify(payload),
      headers: { 'Content-Type': 'application/json' },
    });

    if (!response || !response.ok) {
      const errData = response ? await response.json().catch(() => ({})) : {};
      throw new Error(errData.detail || 'Failed to save data.');
    }

    const resData = await response.json();
    // success: update initial data / UI feedback
    setSuccessMessage(resData.message || 'Data saved successfully!');
    // optionally update initial data and sampleQueries from response if server returns them
  } catch (err) {
    console.error('Save failed', err);
    setError(err.message || 'Save failed');
  }
};


sample_usage: JSON.stringify(sample_usage)



_---------------




const fetchTableMetaData = async () => {
  try {
    const response = await makeApiRequest('/get-table-metadata', {
      body: JSON.stringify({
        market: props.selectedProject,
        table: selectedTable,
      }),
    });

    // If metadata doesn't exist, backend returns 404 — show an empty shape and return
    if (!response) {
      throw new Error('No response from metadata service');
    }

    if (response.status === 404) {
      const blankData = {
        id_key: null,
        description: '',
        tags: [],
        filteredColumns: [],
        aggregateColumns: [],
        sortedColumns: [],
        keyColumns: [],
        sampleQueries: [], // UI friendly field
        // keep shape consistent with setInitialData usage
      };
      console.log('Table metadata not found - returning blankData', blankData);
      setInitialData(blankData);
      return; // EARLY RETURN
    }

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    // parse JSON
    const data = await response.json();

    // backend might return { tables: [...] } or just a table object — normalize
    const result = Array.isArray(data.tables) && data.tables.length > 0
      ? data.tables[0]
      : (data.tables || data); // if data.tables is object or data itself is table

    console.log('Loaded raw metadata result:', result);

    // Normalize sample_usage: backend could send array or JSON-string or null
    let sampleUsageRaw = result.sample_usage ?? result.sampleUsage ?? [];

    if (typeof sampleUsageRaw === 'string' && sampleUsageRaw.trim().length) {
      try {
        sampleUsageRaw = JSON.parse(sampleUsageRaw);
      } catch (e) {
        console.warn('sample_usage is a string but failed to parse JSON, using empty array', e);
        sampleUsageRaw = [];
      }
    }

    if (!Array.isArray(sampleUsageRaw)) {
      // fallback to empty array if unexpected shape
      sampleUsageRaw = [];
    }

    // convert to UI friendly structure
    const loadedSampleUsage = sampleUsageRaw.map((item, idx) => ({
      id: item.id ?? `s-${idx}`,      // keep a unique id for UI operations
      query: item.sql ?? item.query ?? '',   // some backends use 'sql' field
      description: item.description ?? '',
      tempQuery: item.sql ?? item.query ?? '',
      tempDescription: item.description ?? '',
      isEditing: false,
    }));

    const loadedData = {
      id_key: result.id_key ?? null,
      description: result.description ?? '',
      tags: result.tags ?? [],
      filteredColumns: result.filter_columns ?? result.filteredColumns ?? [],
      aggregateColumns: result.aggregate_columns ?? result.aggregateColumns ?? [],
      sortedColumns: result.sort_columns ?? result.sortedColumns ?? [],
      keyColumns: result.key_columns ?? result.keyColumns ?? [],
      sampleQueries: loadedSampleUsage, // matches UI state
      // copy any other fields you need...
    };

    console.log('Loaded while get meta data ->', loadedData);
    setInitialData(loadedData);
  } catch (error) {
    console.error('Error fetching table metadata:', error);
    setError(`Failed to load metadata for ${selectedTable}`);
    setInitialData(null);
  }
};

