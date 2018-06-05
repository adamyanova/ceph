#!/bin/bash

cd /home/adamyanova/src/github.com_adamyanova_ceph_wip-foo 
git checkout wip-foo
git pull
sha1=$(git rev-parse wip-foo)
echo "suite sha1 is: ${sha1} "

sed -i "43s/.*/suite_sha1: ${sha1}/" ../orig.config.yaml 

cd /home/adamyanova/src/go_s3tests
git checkout master
git pull
