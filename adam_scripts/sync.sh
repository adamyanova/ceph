#!/bin/bash

cd /home/adamyanova/src/github.com_adamyanova_ceph_wip-s3tests-go 
git checkout wip-s3tests-go
git pull
sha1=$(git rev-parse wip-s3tests-go)
echo "suite sha1 is: ${sha1} "

#sed -i "43s/.*/suite_sha1: ${sha1}/" ../orig.config.yaml 

cd /home/adamyanova/src/go_s3tests
git checkout master
git pull

cd /home/adamyanova/src/java_s3tests
git checkout master
git pull
