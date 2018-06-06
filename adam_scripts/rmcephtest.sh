#!/bin/bash

host=$1
ssh ubuntu@$host /bin/bash << EOF
sudo yum -y remove golang
rm -rf cephtest/ go/ 
EOF
