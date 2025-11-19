const [dataType, setDataType] = useState('');
const [customDataType, setCustomDataType] = useState('');


useEffect(() => {
  if (!initialData) return;

  setDescription(initialData.description || '');
  setIsFilterable(Boolean(initialData.is_filterable));
  setIsAggregatable(Boolean(initialData.is_aggregatable));
  setSampleValues(Array.isArray(initialData.sample_values) ? initialData.sample_values : []);
  setRelatedTerms(Array.isArray(initialData.related_business_terms) ? initialData.related_business_terms : []);

  const incomingType = (initialData.data_type || '').toString();
  const known = DATA_TYPES.map(d => d.value).includes(incomingType);
  if (known) {
    setDataType(incomingType);
    setCustomDataType('');
  } else if (incomingType) {
    setDataType('other');
    setCustomDataType(incomingType);
  } else {
    setDataType('');
    setCustomDataType('');
  }

  // sample_usage normalisation (if you already have this, keep it)
  const normalizedSampleUsage = (() => {
    const raw = initialData.sample_usage ?? [];
    if (Array.isArray(raw)) return raw.map(it => ({ sql: it.sql??it.query??'', description: it.description??'' }));
    if (typeof raw === 'string') {
      try { return JSON.parse(raw); } catch(e){ return [{ sql: raw, description: '' }]; }
    }
    return [];
  })();
  setSampleUsage(normalizedSampleUsage.map((it, i) => ({ id: it.id ?? `s-${i}`, sql: it.sql, description: it.description, tempQuery: it.sql, tempDescription: it.description, isEditing: false })));
}, [initialData]);

----------



<select
  id="data-type-select"
  value={dataType ?? ''}
  onChange={(e) => {
    const v = e.target.value;
    if (v === 'other') {
      setDataType('other');
      setCustomDataType('');
    } else {
      setDataType(v);
      setCustomDataType('');
    }
  }}
  style={{ padding: '8px 10px', minWidth: 220 }}
>
  {DATA_TYPES.map(dt => (
    <option key={dt.value} value={dt.value}>{dt.label}</option>
  ))}
</select>

{dataType === 'other' && (
  <div style={{ marginTop: 8 }}>
    <input
      type="text"
      placeholder="Enter custom data type (e.g. INT64)"
      value={customDataType || ''}
      onChange={(e) => {
        setCustomDataType(e.target.value);
        setDataType('other');
      }}
      style={{ padding: '8px 10px', minWidth: 220 }}
    />
  </div>
)}







