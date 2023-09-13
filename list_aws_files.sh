#/bin/bash
BASEPATH="s3://cellpainting-gallery/jump"
usage ()
{
  echo -e "\n\nThis script creates txt files with the list of objects in the $BASEPATH AWS bucket."
  echo -e "Usage : $0 ./inputs # To create the files in the ./inputs folder\n\n"
  exit
}
if [[ $# -ne 1 ]] ; then
    usage
    exit 1
fi
mkdir -p $1
sources=`aws s3 ls --profile jump-cp-role "$BASEPATH/" | awk '{print substr($2, 1, length($2)-1)}'`
parallel -j8 aws s3 ls --recursive --profile jump-cp-role "$BASEPATH/{}/" \> $1/{}.txt ::: ${sources[@]}
