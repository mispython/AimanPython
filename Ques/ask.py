def extract_key(filename: str):
    for pattern in PATTERNS:
        m = re.search(pattern, filename)
        if not m:
            continue

        gd = m.groupdict()

        # Try 4-digit year first
        if 'yyyy' in gd and gd['yyyy']:
            year = int(gd['yyyy'])
        else:
            yy = int(gd.get('yy') or 0)
            year = 2000 + yy if yy < 100 else yy

        mm = int(gd.get('mm') or 0)
        dd = int(gd.get('dd') or 0)
        ww = int(gd.get('ww') or 0)

        return (year, mm, ww, dd)

    return None
