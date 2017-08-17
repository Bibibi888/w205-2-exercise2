#!/bin/bash

USER=$1

echo $USER

psql -U ${USER} -d tcount -a -f create_table.sql
