const handleSave = async () => {
  if (!selectedTable || !selectedColumn) {
    setError('Please select a table and a column before saving.');
    return;
  }

  setIsLoading(true);
  setError(null);
  setSuccessMessage('');

  const payload = {
    id_key: initialData?.id_key ?? null,
    market: props.selectedProject,
    data_namespace: initialData?.data_namespace ?? '',
    table: selectedTable,
    column: selectedColumn,
    obj: {
      description: description || '',
      data_type: dataType || '',
      is_filterable: Boolean(isFilterable),
      is_aggregatable: Boolean(isAggregatable),
      sample_values: Array.isArray(sampleValues) ? sampleValues : [],
      related_business_terms: Array.isArray(relatedTerms) ? relatedTerms : [],
      sample_usage: Array.isArray(sampleUsage)
        ? sampleUsage.map(q => ({ sql: (q.sql || '').trim(), description: (q.description || '').trim() }))
        : [],
      updated_by: 'gaurav'
    }
  };

  try {
    const response = await makeApiRequest('/update-column-metadata', {
      method: 'POST',
      body: JSON.stringify(payload),
      headers: { 'Content-Type': 'application/json' },
    });

    if (!response || !response.ok) {
      const errData = response ? await response.json().catch(() => ({})) : {};
      throw new Error(errData.detail || 'Failed to save data.');
    }

    const data = await response.json();
    setSuccessMessage(data.message || 'Data saved successfully!');

    const savedData = {
      description: payload.obj.description,
      data_type: payload.obj.data_type,
      is_filterable: payload.obj.is_filterable,
      is_aggregatable: payload.obj.is_aggregatable,
      sample_values: payload.obj.sample_values,
      related_business_terms: payload.obj.related_business_terms,
      sample_usage: payload.obj.sample_usage,
    };

    setInitialData(prev => ({ ...(prev || {}), ...savedData }));
    onSave && onSave(data);
  } catch (err) {
    setError(`${err.message}`);
  } finally {
    setIsLoading(false);
    setTimeout(() => setSuccessMessage(''), 5000);
  }
};

_-----------


const DATA_TYPES = [
  { value: '', label: 'Select data type' },
  { value: 'text', label: 'text' },
  { value: 'varchar', label: 'varchar' },
  { value: 'integer', label: 'integer' },
  { value: 'bigint', label: 'bigint' },
  { value: 'smallint', label: 'smallint' },
  { value: 'boolean', label: 'boolean' },
  { value: 'numeric', label: 'numeric' },
  { value: 'real', label: 'real' },
  { value: 'double precision', label: 'double precision' },
  { value: 'timestamp', label: 'timestamp' },
  { value: 'date', label: 'date' },
  { value: 'time', label: 'time' },
  { value: 'json', label: 'json' },
  { value: 'jsonb', label: 'jsonb' },
  { value: 'array', label: 'array' },
  { value: 'other', label: 'Other (custom)' }
];

{/* Place this JSX where the input should appear */}
<div style={{ marginTop: 8 }}>
  <label htmlFor="data-type-select" style={{ display: 'block', marginBottom: 6 }}>Data Type</label>

  <select
    id="data-type-select"
    value={dataType ?? ''}
    onChange={(e) => setDataType(e.target.value)}
    style={{ padding: '8px 10px', minWidth: 220 }}
  >
    {DATA_TYPES.map(dt => (
      <option key={dt.value} value={dt.value}>{dt.label}</option>
    ))}
  </select>

  {/* show an input if user chooses "other" so they can type any custom type */}
  {dataType === 'other' && (
    <div style={{ marginTop: 8 }}>
      <input
        type="text"
        placeholder="Enter custom data type (e.g. character varying(255))"
        value={customDataType || ''}
        onChange={(e) => {
          setCustomDataType(e.target.value);
          setDataType(e.target.value); // keep the actual dataType state in sync
        }}
        style={{ padding: '8px 10px', minWidth: 220 }}
      />
    </div>
  )}
</div>


const [customDataType, setCustomDataType] = useState('');

