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
