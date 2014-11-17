#!/usr/bin/env bash
declare -a output_files=(xml_files csv_files yml_files)

find ${output_files[@]} -type f | grep -v .gitkeep | xargs rm
