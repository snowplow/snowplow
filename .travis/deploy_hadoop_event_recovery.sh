#!/bin/bash

echo "--- beginning deployment ---"

cd $TRAVIS_BUILD_DIR
./event-recovery-ci.bash
