#!/bin/bash

targets=$(teuthology-lock --list-targets)
substr='front.sepia.ceph.com'

for t in $targets
do
  if [[ "$t" == *"$substr"* ]] ; then
    echo "teutology-lock --unlock ${t::-1}"
    teuthology-lock --unlock ${t::-1}
  fi

done

