import re

def parse_key_value_text(raw):
    """Convert raw text like 'table=event_store\market=AMH_OB' into dict"""
    result = {}
    
    # split by backslash
    parts = raw.split("\\")
    
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            result[k.strip()] = v.strip()
    
    return result
