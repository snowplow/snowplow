#!/bin/bash

echo "--- beginning deployment ---"

cd $TRAVIS_BUILD_DIR
./evet-recovery-ci.bash
