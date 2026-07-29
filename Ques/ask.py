PATTERNS = [
    # YYYYMMDD (added)
    r"(?P<prefix>[a-zA-Z]*)(?P<year>\d{4})(?P<mm>\d{2})(?P<dd>\d{2})",
    # Existing patterns
    r"(?P<prefix>[a-zA-Z]*)(?P<yy>\d{2})(?P<mm>\d{2})(?P<dd>\d{2})",
    # ... others ...
]

def extract_key(filename: str):
    for pattern in PATTERNS:
        m = re.search(pattern, filename)
        if not m:
            continue
        gd = m.groupdict()

        if 'year' in gd:
            year = int(gd['year'])
        else:
            yy = int(gd.get('yy') or 0)
            year = 2000 + yy if yy < 100 else yy

        mm = int(gd.get('mm') or 0)
        dd = int(gd.get('dd') or 0)
        ww = int(gd.get('ww') or 0)

        return (year, mm, ww, dd)

    return None
