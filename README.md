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
