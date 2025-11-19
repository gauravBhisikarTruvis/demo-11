const handleSaveQuery = (id) => {
  setSampleUsage(prev =>
    prev.map(q =>
      q.id === id
        ? {
            ...q,
            sql: q.tempQuery.trim(),
            description: q.tempDescription.trim(),
            isEditing: false,
          }
        : q
    )
  );
};



const handleAddNewQuery = () => {
  if (!newQueryValue.trim()) return;

  setSampleUsage(prev => [
    ...prev,
    {
      id: `new-${Date.now()}`,
      sql: newQueryValue.trim(),
      description: newDescriptionValue.trim(),
      tempQuery: newQueryValue.trim(),
      tempDescription: newDescriptionValue.trim(),
      isEditing: false
    }
  ]);

  setNewQueryValue("");
  setNewDescriptionValue("");
};
