def flatten(self, items):
    flat = []
    for item in items:
        if isinstance(item, list):
            flat.extend(self.flatten(item))
        else:
            flat.append(item)
    return flat
