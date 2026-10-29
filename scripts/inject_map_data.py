import json
import os
import re

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
points = json.load(open(os.path.join(BASE, "eda", "module4_points.json"), encoding="utf-8"))
payload = json.dumps(points, ensure_ascii=False, separators=(",", ":"))

path = os.path.join(BASE, "dashboard", "subset.html")
html = open(path, encoding="utf-8").read()
new, n = re.subn(r"/\*MAPDATA\*/.*?/\*ENDMAPDATA\*/",
                 lambda m: "/*MAPDATA*/" + payload + "/*ENDMAPDATA*/", html, count=1, flags=re.S)
assert n == 1, "marker not found in subset.html"
open(path, "w", encoding="utf-8").write(new)
print(f"injected {len(points['x'])} points ({len(payload)//1024} KB) into dashboard/subset.html")
