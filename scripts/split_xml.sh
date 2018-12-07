#! /bin/bash

FILE=${1}
LINES=${2}
ROWTAG=${3}

# First split the files in chunks of LINES number of lines
split -l ${LINES} ${FILE} ${FILE}-

# Create the destination folder for the the splitted files and move them there
DEST=$(dirname "${FILE}")/split/$(basename ${FILE})
rm -rf ${DEST}
mkdir -p ${DEST}
mv $(dirname "${FILE}")/${FILE}-* ${DEST}/

#Add XML header and footer to each file
TOTAL_PARTS=$(ls ${DEST}/* | wc -l)
part=0
for f in ${DEST}/*
do 
  if [[ -f ${f} ]]
  echo "Processing part ${part} of ${TOTAL_PARTS}"
  then
    # Need to create another file to be able to prepend the header
    PART_FILE=${DEST}/part-${part}.xml
    touch -f ${PART_FILE}
    # Add header only if it is not already present (1st partition)
    if [[ $(head -n1 ${f}) != *"xml version"* ]]
    then
      echo "<?xml version="1.0" encoding="utf-8"?>" > ${PART_FILE}
      echo "<${ROWTAG}>" >> ${PART_FILE}
    fi
    # Copy file content
    cat ${f} >> ${PART_FILE}
    # Add footer only if it is not already present (last partition)
    if [[ $(tail -n1 ${f}) != *"${ROWTAG}"* ]]
    then
      echo "</${ROWTAG}>" >> ${PART_FILE}
    fi
    # Remove origin file
    rm ${f}
    # HACK: For spark-xml to work each element needs to have an inner child
    # Distinguish between mac and linux
    if [[ $(uname -s) == "Darwin" ]]
    then
      sed -i '' 's/ \/>/><t>t<\/t><\/row>/g' ${PART_FILE}
    else
      sed -i 's/ \/>/><t>t<\/t><\/row>/g' ${PART_FILE}
    fi
    # sed -i '' 's/ \/>/><\/row>/g' ${PART_FILE}
    part=$((part+1))
  fi
done
