#! /bin/bash

usage() {
  echo "Usage: $0 bucketname filename"
  exit 1
}

if [ $# -ne 2 ] ; then
  usage
else
  BUCKET=${1}
  KEY=${2}
fi

while true; do
  UPLOAD_ID=$(aws s3api list-multipart-uploads --bucket ${BUCKET} --key-marker ${KEY} --max-uploads 1 2> /dev/null \
    | jq -r '.Uploads[0].UploadId')
  if [[ -n "${UPLOAD_ID}" && "${UPLOAD_ID}" != "null" ]]; then
    break
  fi
  sleep 1
  echo -n "."
done
watch -n 1 "aws s3api list-parts --bucket ${BUCKET} --key ${KEY} --upload-id ${UPLOAD_ID} | jq '[.Parts[].Size] | add'"
