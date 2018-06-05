#!/bin/bash


# Clean teuthology logs
echo "Removing ./teuthology_logs"
rm -rf ./teuthology_logs


# Cleanup leftover directories from locked machines
targets=$(teuthology-lock --list-targets)
substr='front.sepia.ceph.com'

for t in $targets
do
  if [[ "$t" == *"$substr"* ]] ; then
    echo "Running: ubunutu@${t::-1}\$ rm -rf cephtest/ go/"
    ./rmcephtest.sh ${t::-1}
  fi
done

# Start teuthology job on locked machies
echo "Strating teuthology job"
teuthology targets.yaml orig.config.yaml --verbose --archive ./teuthology_logs

