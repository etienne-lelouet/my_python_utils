#! /usr/bin/python3

import sys
import site

if __name__ == "__main__":
    paths = site.getsitepackages()
    if len(paths) == 0:
        print("Could not find local library path.", file=sys.stderr)
        sys.exit(1)
    
    for path in paths:
        if "local" in path:
            print(path)
            sys.exit(0)
    
    print("Could not find local library path.", file=sys.stderr)
    sys.exit(1)