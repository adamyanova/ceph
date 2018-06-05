#!/bin/bash

host=$1
ssh ubuntu@$host /bin/bash << EOF
rm -rf cephtest/ go/ 
EOF
