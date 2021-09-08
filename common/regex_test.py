import re

t = "SITE COORDINATES:25.938580' -80.210910"

regx = re.compile(r"-?[0-9]+\.[0-9]+")
groups = re.findall(regx, t)
print(groups)

