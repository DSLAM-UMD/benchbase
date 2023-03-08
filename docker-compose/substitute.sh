#!/bin/bash
set -eux

template="/${BENCHMARK}-config-template.xml"
out="/${BENCHMARK}-config.xml"

cp $template $out
sed -i "s/BB_HOST/${HOST}/" $out
sed -i "s/BB_PORT/${PORT}/" $out 
sed -i "s/BB_DATABASE/${DATABASE}/" $out 
sed -i "s/BB_USERNAME/${USERNAME}/" $out 
sed -i "s/BB_PASSWORD/${PASSWORD}/" $out 
sed -i "s/BB_REGION/${REGION}/" $out 
