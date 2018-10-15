import clr
import os

DIR = "Builds/"

for filename in os.listdir('.'):
    print(filename)
    if not os.path.isfile(filename):
        continue
    splits = os.path.splitext(filename)
    if splits[1] == ".py":
        name = splits[0]
        print(name)
        if name == 'make_dll':
            continue
        clr.CompileModules(DIR+name+".dll", filename)

    