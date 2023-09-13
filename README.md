# data-validation

Scripts to validate data uploaded to `s3://cellpainting-gallery`.
It requires python 3.10+.

We want to do 3 things

1. Completeness: Is the data complete and is everything where it should be?
2. Consistency: Is the file damaged?
3. Correctness: Have the data been processed correctly?

At present, this repo

- focuses on **completeness** only
- only checks for the existence of files based on [jsonschema](https://json-schema.org/) validation.
- partially checks their sizes. See [`schema.json`](schema.json#L166) file, `"$defs"."s3_object"."size"` object for more info.
- **does not** check file content other than platemap, barcode and raw profiles.

## Setup

We use [mamba](https://mamba.readthedocs.io/en/latest/) to manage the computational environment.

To install mamba see [instructions](https://mamba.readthedocs.io/en/latest/installation.html).

After installing mamba, execute the following to install and navigate to the environment:

```bash
# First, install the `genemod` conda environment
mamba env create --force --file environment.yml

# If you had already installed this environment and now want to update it
mamba env update --file environment.yml --prune

# Then, activate the environment and you're all set!
mamba activate dataval
```

## 1. Download data from aws

### 1.1 Get list of files

Run `list_aws_files.sh` to list all the S3 objects ("files") in each source folder within the `jump` project folder and save these lists as separate .txt files within the `inputs` folder:

```bash
./list_aws_files.sh inputs
```

If you want to create/update a single source (e.g. `source_4`), instead of running `list_aws_files.sh`, use:

```bash
aws s3 ls --recursive --profile jump-cp-role "s3://cellpainting-gallery/jump/source_4/" > inputs/source_4.txt
```

### 1.2 Get metadata and profiles

To download metadata and profiles in the `inputs` folder, run:

```bash
./download_data.sh inputs
```

If you want to get/update data from a single source (e.g. `source_4`), instead of running `download_data.sh`, use:

```bash
aws s3 cp --recursive --profile jump-cp-role "s3://cellpainting-gallery/jump/source_4/workspace/metadata" inputs/source_4/workspace/metadata
aws s3 cp --recursive --profile jump-cp-role "s3://cellpainting-gallery/jump/source_4/workspace/profiles" inputs/source_4/workspace/profiles
```

## 2. Run validation

### 2.1 Configure

Edit theses variables in [`config.json`](config.json) to match your local setup:

```json
    "aws_prefix": "jump/",
    "cpg_id": "cpg0016",
    "mandatory_columns_path": "mandatory_columns/cpg0016.txt",
    "local_copy_path": "./inputs/",
```

- `aws_prefix`: path in the "s3://cellpainting-gallery/" bucket where the `source_X` folder lives. More info at [folder structure](https://github.com/jump-cellpainting/aws/blob/main/DATA_UPLOAD.md#complete-folder-structure).
- `cpg_id`: ID for the dataset. Any of the [datasets](https://github.com/jump-cellpainting/datasets/#details-about-the-data) in the Cell Painting Gallery.
- `mandatory_columns_path`: Path to a text file containing the list of features every profile should have.
- `local_copy_path`: Path to the input data containing the list of S3 objects along with the metadata and the profiles. These files are downloaded from [step 1](https://github.com/jump-cellpainting/data-validation/blob/main/README.md#1-download-data-from-aws).

### 2.2 Create `structure.json` files

[create_structure.py](create_structure.py) converts the aws file list into a structured json file. To run all of the sources at once:

```bash
ls inputs/*txt | parallel -j8 python create_structure.py {}
```

In addition to the `structure.json` file, this process will generate `outputs/{SOURCE_ID}/unknown_objects.csv` containing S3 objects that don't match the [expected folder structure](https://github.com/jump-cellpainting/aws/blob/main/DATA_UPLOAD.md#complete-folder-structure).

### 2.3 Validate structure

[`create_validated_structure.py`](create_validated_structure.py) creates a new structure.json file after filtering invalid elements (i.e. plates and batches)

```bash
find outputs/ -name "structure.json" -exec python create_validated_structure.py {} \;
```

**Watch out the output!**. It will describe which plates/batches were discarded and why.

### 2.3 Prepare data to be uploaded in the public aws folder

[`prepare_upload.py`](prepare_upload.py) creates a new folder (`./clean` as default) where only the valid plates with the minimum set of features and metadata is added. More info at <https://github.com/jump-cellpainting/data-validation/issues/11>

```bash
find outputs/ -name "structure_validated.json" -exec python prepare_upload.py {} \;
```

Then use `aws sync` to push the data to the public repo.

## 3. Create collated files

### 3.1 create `well.csv.gz`

[`create_collated_wells.py`](create_collated_wells.py) reads json files and [jcpids](jcpids/) metadata files to create a collated table containing the JCP2022 identifier for each of the wells in the dataset.

```bash
python create_collated_wells.py $(find outputs/ -name "structure_validated.json")
```

### 3.2 create `plate.csv.gz`

[`create_collated_plates.py`](create_collated_plates.py) reads file created in [step 3.1](#31-create-wellcsvgz) and to create a collated table containing the metadata for each of the plates in the dataset.

```bash
python create_collated_plates.py ./outputs
```

### 3.3 Update previous collated files

When a dataset already exists, you may want to insert and/or update current collated files.
[`upsert.py`](upsert.py) script allows to [upsert](https://en.wikipedia.org/wiki/Merge_(SQL)#Synonymous) records into an existing database:

Update wells:

```bash
python upsert.py \
  ../datasets/metadata/well.csv.gz \ # Current database
  outputs/well.csv.gz \ # records to update/insert
  outputs/well_new.csv.gz \ # Path to save the new collated file
  Metadata_Source Metadata_Plate Metadata_Well # Fields used to merge both databases

# Upsert completed.
```

Update plates:

```bash
python upsert.py\
  ../datasets/metadata/plate.csv.gz \ # Current database
  outputs/plate.csv.gz \ # records to update/insert
  outputs/plate_new.csv.gz \ # Path to save the new collated file
  Metadata_Source Metadata_Batch Metadata_Plate # Fields used to merge both databases
```

## Addendum

Here are all the instructions at one go, for a single source.

Warning: This was created for convenience but these instructions might go out of sync with the instructions above!

```sh
# pipenv install  # do this once
mamba activate dataval

DATASET=jump # update this
DATASET_DEST=cpg0016-jump # update this; this might be different from DATASET if the data needs to be moved to a new location
SOURCE=source_1 # update this

INPUTS=inputs/${DATASET}
OUTPUTS=outputs/${DATASET}

rm -rf ${INPUTS}/${SOURCE}*
rm -rf ${OUTPUTS}/${SOURCE}

mkdir -p ${INPUTS}/${SOURCE}
mkdir -p ${INPUTS}/${SOURCE}/workspace/metadata
mkdir -p ${OUTPUTS}/${SOURCE}

aws s3 ls --recursive --profile jump-cp-role "s3://cellpainting-gallery/${DATASET}/${SOURCE}/" > ${INPUTS}/${SOURCE}.txt
aws s3 cp --recursive --profile jump-cp-role "s3://cellpainting-gallery/${DATASET}/${SOURCE}/workspace/metadata" ${INPUTS}/${SOURCE}/workspace/metadata
aws s3 cp --recursive --profile jump-cp-role --exclude "*_normalized*" --exclude "*feature_select*" --exclude "*augmented*" --exclude "*sqlite*" "s3://cellpainting-gallery/${DATASET}/${SOURCE}/workspace/profiles" ${INPUTS}/${SOURCE}/workspace/profiles

python create_structure.py \
  --output_dir ${OUTPUTS} \
  ${INPUTS}/${SOURCE}.txt

python create_validated_structure.py \
  --output ${OUTPUTS}/${SOURCE}/structure_validated.json \
  ${OUTPUTS}/${SOURCE}/structure.json

python prepare_upload.py \
  --output ${OUTPUTS}/clean \
  ${OUTPUTS}/${SOURCE}/structure_validated.json

aws s3 sync \
  --profile jump-cp-role-jump-cellpainting --acl bucket-owner-full-control \
  ${OUTPUTS}/clean/${SOURCE}/workspace/profiles \
  s3://cellpainting-gallery/${DATASET_DEST}/${SOURCE}/workspace/profiles

python create_collated_wells.py \
  --output ${OUTPUTS} \
  ${OUTPUTS}/${SOURCE}/structure_validated.json


# check for errors and missing wells
# Both these files should be empty

zcat ${OUTPUTS}/well_errors.csv.gz
zcat ${OUTPUTS}/well_missing.csv.gz

python \
  create_collated_plates.py \
    ./${OUTPUTS}

zcat ${OUTPUTS}/plate.csv.gz | head -n 5

python upsert.py \
  ../datasets/metadata/well.csv.gz \
  ${OUTPUTS}/well.csv.gz \
  ${OUTPUTS}/well_new.csv.gz \
  Metadata_Source Metadata_Plate Metadata_Well

python upsert.py\
  ../datasets/metadata/plate.csv.gz \
  ${OUTPUTS}/plate.csv.gz \
  ${OUTPUTS}/plate_new.csv.gz \
  Metadata_Source Metadata_Batch Metadata_Plate


# copy the update files to the datasets repo
cp ${OUTPUTS}/well_new.csv.gz ../datasets/metadata/well.csv.gz

cp ${OUTPUTS}/plate_new.csv.gz ../datasets/metadata/plate.csv.gz

# now create a new branch in the datasets repo, commit these files, and create a PR
```
