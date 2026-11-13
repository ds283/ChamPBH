#!/bin/bash
cd /Users/ds283/Documents/Code/ChamPBH
A=/private/tmp/claude-35086/-Users-ds283--claude-spares-spare-L2vpmA/ebde1a9d-136d-40c1-b0a9-b34bf63b08de/scratchpad/ewe-03/agent
run() { name=$1; shift; echo "== $name start $(date '+%F %T') $(uptime)" >> $A/suites.log; s=$(date +%s); PYTHONPATH=. ./venv/bin/python -m unittest discover "$@" > $A/$name.out 2>&1; rc=$?; e=$(date +%s); echo "== $name rc=$rc elapsed=$((e-s))s end $(uptime)" >> $A/suites.log; tail -4 $A/$name.out >> $A/suites.log; }
run cosmology -s CosmologyModels/tests -t .
run objectfactories -s ObjectFactories/tests -t .
run rundrivers -s RunDrivers/tests -t .
run ct_af -s ComputeTargets/tests -t . -p 'test_[a-f]*.py'
run ct_gz -s ComputeTargets/tests -t . -p 'test_[g-z]*.py'
run tools -s tools/tests -t .
echo ALLDONE >> $A/suites.log
