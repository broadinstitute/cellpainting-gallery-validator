"""Tests"""
import pytest

from jump.parsing import process_line, datasets


@pytest.fixture(autouse=True, scope="function")
def reset_messages():
    """Clearing validation and dataset for every test"""
    datasets.clear()


def test_line_1():
    """Test analysis file"""
    line = (
        "2021-11-02 18:36:12    5310570 "
        "jump/source_6/workspace/analysis/p210830CPU2OS48hw384exp023JUMP/"
        "110000293094/analysis/110000293094-A01-1/Cells.csv"
    )
    process_line(line)


def test_line_2():
    """Test profile file"""
    line = (
        "2021-11-03 05:15:47    1639645 "
        "jump/source_6/workspace/profiles/p210830CPU2OS48hw384exp023JUMP/"
        "110000293094/110000293094_normalized_feature_select_negcon_batch.csv"
    )
    process_line(line)


def test_line_3():
    """Test profile file"""
    line = (
        "2021-11-03 05:15:47    1481504 "
        "jump/source_6/workspace/profiles/p210830CPU2OS48hw384exp023JUMP/"
        "110000293094/110000293094_normalized_feature_select_negcon_batch.csv.gz"
    )
    process_line(line)


def test_line_4():
    """Test image file"""
    line = (
        "2021-11-10 08:07:26    2226012 "
        "jump/source_3/images/CP_25_all_Phenix1/images/"
        "C13443aW__2021-09-18T06_57_42-Measurement1/Images/r07c21f02p01-ch1sk1fk1fl1.tiff"
    )
    process_line(line)


def test_line_5():
    """Test outline file"""
    line = (
        "2021-11-10 16:03:49      24255 "
        "jump/source_3/workspace/analysis/CP_25_all_Phenix1/C13451aW/"
        "analysis/C13451aW-F08/outlines/F08_s3--nuclei_outlines.png"
    )
    process_line(line)


def test_line_6():
    """Test outline file"""
    line = (
        "2021-11-17 07:48:21      41868 "
        "jump/source_10/workspace/analysis/2021_05_31_U2OS_48_hr_run1/"
        "Dest210531-152324/analysis/J10/outlines/J10_s1--cell_outlines.png"
    )
    process_line(line)


def test_line_7():
    """Test analysis file"""
    line = (
        "2021-11-17 07:54:37     297017 jump/source_10/workspace/analysis/"
        "2021_05_31_U2OS_48_hr_run1/Dest210531-152810/analysis/L24/Image.csv"
    )
    process_line(line)


def test_line_8():
    """Test analysis file"""
    line = (
        "2021-11-17 07:55:16     297634 "
        "jump/source_10/workspace/analysis/2021_05_31_U2OS_48_hr_run1/"
        "Dest210531-152945/analysis/N03/Image.csv"
    )
    process_line(line)


def test_line_9():
    """Assert incorrect path"""
    line = (
        "2021-10-23 00:56:59    2063698 jump/source_11/images/Batch4/"
        "EC000111__2021-09-20T14_39_07-Measurement1/Images/r14c09f01p01-ch1sk1fk1fl1.tiff"
    )
    with pytest.raises(ValueError):
        process_line(line)


def test_line_10():
    """Test illum file"""
    line = (
        "2021-11-09 21:00:16    3984144 "
        "jump/source_5/images/JUMPCPE-20210623-Run01_20210624_003152/"
        "illum/ADMJUM001/ADMJUM001_IllumAGP.npy"
    )
    process_line(line)


def test_line_11():
    """Test cell and nuclei outline files"""
    line_1 = (
        "2021-11-10 22:59:32      32524 jump/source_3/workspace/"
        "analysis/CP_31_all_Phenix1/B040303a/analysis/"
        "B040303a-E05/outlines/E05_s4--cell_outlines.png"
    )
    line_2 = (
        "2021-11-10 22:59:32      15205 jump/source_3/workspace/"
        "analysis/CP_31_all_Phenix1/B040303a/analysis/"
        "B040303a-E05/outlines/E05_s4--nuclei_outlines.png"
    )
    process_line(line_1)
    process_line(line_2)


def test_line_12():
    """Test incorrect format"""
    line = (
        "2022-01-14 11:41:28      10758 jump/source_2/workspace/"
        "analysis/20211003_Batch_13/1086289792/analysis/"
        "1086289792/outlines/1086289792_P24_6_nuclei_outlines.png"
    )
    process_line(line)


def test_line_13():
    """Test incorrect format"""
    line = (
        "2022-01-13 20:45:39 12920465613 jump/source_2/workspace/"
        "analysis/20210607_Batch_2/1053601756/analysis/"
        "1053601756/Cells.csv"
    )
    process_line(line)


def test_line_14():
    """Test plate with underscore in name format"""
    line = (
        "2021-10-06 00:06:45    1994752 "
        "jump/source_5/images/JUMPCPE-20210623-Run01_20210624_003152/"
        "images/P01_ADMJUM001/P01_ADMJUM001_A01_T0001F001L01A03Z01C03.tif"
    )
    process_line(line)


def test_line_15():
    """Test undetected site"""
    line = (
        "2021-11-17 15:45:43      33689 "
        "jump/source_10/workspace/analysis/2021_06_14_U2OS_48_hr_run5/"
        "Dest210614-163621/analysis/O09/outlines/O09_s1--cell_outlines.png"
    )
    process_line(line)


def test_line_16():
    """Test plate_id wrongly detected"""
    line = (
        "2022-03-07 15:18:55    2765104 "
        "jump/source_7/images/2021_07_19_JUMP-SC-Run1/illum/IllumAGP.tif"
        "/IllumAGP.tif"
    )
    with pytest.raises(
        ValueError, match="Invalid format for Illumination Correction. Expected .npy"
    ):
        process_line(line)


def test_line_17():
    """Test site file with takeda format"""
    line = (
        "2022-03-31 04:28:09      72779 "
        "jump/source_11/workspace/analysis/Batch2/LM37-70_1/analysis/"
        "LM37-70_1-A01-2/Image.csv"
    )
    process_line(line)


def test_line_18():
    """Test line with partial plate name in csv file"""
    line = (
        "2022-03-31 01:50:01    3643780 "
        "jump/source_11/workspace/analysis/Batch3/EC000157/analysis/EC000157real-A10-3/Nuclei.csv"
    )
    process_line(line)


def test_line_19():
    """Test image wrong image"""
    line = (
        "2022-03-11 07:45:58     178899 "
        "jump/source_11/images/Batch2/images/LM37-70_1__2021-06-06T16_25_09-Measurement1/Assaylayout/Unnamed.xml"
    )
    process_line(line)


def test_line_20():
    """Test plate with underscore and hyphen"""
    line = (
        "2022-03-11 07:45:58    2247103 "
        "jump/source_11/images/Batch2/images/LM37-70_1__2021-06-06T16_25_09-Measurement1/Images/r01c01f02p01-ch1sk1fk1fl1.tiff"
    )
    process_line(line)


def test_line_21():
    """Test Cytoplasm file"""
    line = (
        "2022-03-12 14:48:34    3812564 "
        "jump/source_11/workspace/analysis/Batch3/EC000157real/analysis/EC000157real-A01-1/Cytoplasm.csv"
    )
    process_line(line)


def test_line_22():
    """Test csv with gz suffix"""
    line = (
        "2022-05-25 15:33:00 7195492927 "
        "jump/source_7/workspace/analysis/20210719_Run1/CP1-SC1-01/analysis/CP1-SC1-01/Cells.csv.gz"
    )
    process_line(line)


def test_line_23():
    line = "2021-12-06 15:38:47         11 jump/source_8/workspace/profiles/README.md\n"
    with pytest.raises(ValueError, match="Object is not a profile:"):
        process_line(line)


def test_line_24():
    line = "2022-06-21 19:20:22    4665728 cpg0000-jump-pilot/source_4/images/2020_11_04_CPJUMP1/illum/BR00116991/BR00116991_IllumHighZBF.npy"
    process_line(line)


def test_line_25():
    line = "2023-02-09 17:47:37   22161878 cpg0000-jump-pilot/source_4/workspace/profiles/2020_11_04_CPJUMP1_DL/BR00116996/BR00116996.csv.gz"
    process_line(line)


if __name__ == "__main__":
    test_line_25()
