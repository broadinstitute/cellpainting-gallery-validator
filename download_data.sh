BASEPATH="s3://cellpainting-gallery/jump"
usage ()
{
  echo -e "\nThis script downloads metadata and profile data from the $BASEPATH AWS bucket."
  echo -e "Usage : $0 ./inputs # To download the files in the ./inputs folder \n\n"
  exit
}
if [[ $# -ne 1 ]] ; then
    usage
    exit 1
fi
mkdir -p $1
# sources=("source_2" "source_3" "source_4" "source_5" "source_6" "source_7" "source_8" "source_10")
# for source_id in ${sources[@]};
sources=`aws s3 ls --profile jump-cp-role "$BASEPATH/" | awk '{print substr($2, 1, length($2)-1)}'`
for source_id in $sources;
do
    aws s3 cp --recursive --profile jump-cp-role "$BASEPATH/$source_id/workspace/metadata" $1/$source_id/workspace/metadata
    aws s3 cp --recursive --profile jump-cp-role --exclude "*_normalized*" --exclude "*feature_select*" --exclude "*augmented*" "$BASEPATH/$source_id/workspace/profiles" $1/$source_id/workspace/profiles
done
