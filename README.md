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
