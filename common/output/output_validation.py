import pandas as pd
import os
import glob


def match_common_files():
    df = pd.read_csv(r'./ocr-poc-test-data.csv')
    csv_files = df['Link to as built'].apply(lambda url: (os.path.basename(url)))
    df['file_name'] = csv_files
    files = glob.glob(r'/Users/ujjwal/projects/cci/data/as-builts/pdf/*.pdf')
    dir_fnames = []
    for f in files:
        fname = os.path.basename(f)
        if fname in csv_files.values:
            print (fname)
            dir_fnames.append(fname)

    df['is_test'] = df['file_name'].apply(lambda v: v in dir_fnames)
    df_sub = df[df['is_test']]
    df_sub.to_csv(r'./ocr-poc-test-data-sub.csv', index=False)

    """
    CH1509BA_91LAB_Elevation_As_Built - Node.pdf
    CHPH30587_SCU - 469511_Redlines - Node.pdf
    CHPH32493_SCU - 470217_NAT_Reviewed_Redline - Node.pdf
    CH2300BA_11LAB_Elevation_NAT_Reviewed_As_Built.pdf
    CHPH30788_469865_NAT_Redlined_As - Built.pdf
    CH1424BA_81LAB_Elevation_As_Built.pdf
    CH2262BA_21LAB_Elevation_As_Built.pdf
    """


def compare_output():
    test_file = 'OCR_100SiteTest.csv'
    output_file = 'colo_test_set_output.csv'

    test_df = pd.read_csv(test_file)
    output_df = pd.read_csv(output_file)

    test_df['file'] = test_df['Link to as built'].apply(lambda x: str(os.path.basename(x)))
    output_df['file'] = output_df['file'].apply(lambda x: str(x))

    test_df = test_df[["file", "Pole Height - AGL"]]
    output_df = output_df[["file", "pole height"]]
    output_df = output_df[ output_df["pole height"] > 0]

    test_df['Pole Height - AGL'] = test_df['Pole Height - AGL'].apply(lambda x: "%.2f" % float(x))
    output_df['pole height'] = output_df['pole height'].apply(lambda x: "%.2f" % float(x))

    test_df.to_csv('./test.csv', index=False)
    output_df.to_csv('./output.csv', index=False)

    join_df = test_df.merge(output_df, on='file')
    join_df["status"] = join_df.apply(lambda x: x['pole height'] == x['Pole Height - AGL'], axis=1)
    join_df.to_csv('./merged.csv', index=False)



# match_common_files()
compare_output()

