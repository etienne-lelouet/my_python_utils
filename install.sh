#!/bin/bash

if python3 -c "import fabric" > /dev/null; then
    echo "Fabric is already installed."
else
    echo "Installing Fabric"
    apt install python3-fabric
fi

if [ -z $1 ]; then
    echo "No installation path provided"
    exit 1
fi

if [ ! -d "$1" ]; then
    echo "Provided path is not a directory"
    exit 1
fi


echo "Installing to $1"

cp src/async_fs_utils.py "$1"
cp src/async_process_utils.py "$1"

echo "Testing installation..."
python3 -c "import async_fs_utils; import async_process_utils"

if [ $? -ne 0 ]; then
    echo "Installation failed."
    exit 1
fi

echo "Installation succeeded."
