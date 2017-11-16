
import re

v = 'Memory Lane (2010-)'

x = re.findall(r'\((.*)\)', v)[0]
y = re.findall(r'\d+', x)[0]
print(y)