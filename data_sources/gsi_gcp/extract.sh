#!/opt/local/bin/bash -eu

# Extract GCP xml files from ALL1 zip files

rm -rf workspace/*

for c in private_data/FG-GML-*-ALL1-*.zip; do
  echo "Extracting $c"
  tmp=${c##*/}
  unzip -q -d workspace/${tmp%.zip} $c '*-01-*'
done

declare -A zip01

for z in workspace/*/FG-GML-*-01-*.zip; do
  b=${z##*/}
  if [ -z "${zip01[$b]:-}" ]; then
    echo "Extracting $z"
    set +e
    unzip -q -d workspace $z '*-GCP-*'
    set -e
    zip01[$b]=$z
  fi
done

# Extract ElevPt xml files from ALL2 zip files

for c in private_data/FG-GML-*-ALL2-*.zip; do
  echo "Extracting $c"
  tmp=${c##*/}
  unzip -q -d workspace/${tmp%.zip} $c '*-09-*'
done

declare -A zip09

for z in workspace/*/FG-GML-*-09-*.zip; do
  b=${z##*/}
  if [ -z "${zip09[$b]:-}" ]; then
    echo "Extracting $z"
    set +e
    unzip -q -d workspace $z '*-ElevPt-*'
    set -e
    zip09[$b]=$z
  fi
done
