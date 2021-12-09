
from databaker.framework import *
import pandas as pd
import datetime, glob

## these needed to be pointed to if not pip installed 
from databakerUtils.sparsityFunctions import SparsityFiller
from api_pipeline import *

### file paths that may need to be changed ###
location = 'inputs/uk-spending-on-cards/*.xlsx' # path to source data
output = 'D:/' # output file saved here
credentials = 'florence-details.json' # path to login details
metadata_file = 'inputs/uk-spending-on-cards/uk-spending-on-cards-time-series-v2.csv-metadata.json' # path to metadata file

def transform(location, output):
    file = glob.glob(location)
    assert len(file) == 1, f"should onlybe one input file not '{len(file)}'"
    file = file[0]
    output_file = output + 'v4-uk-spending-on-cards.csv'

    tabs_we_want = 'Daily CHAPS indices'
    tabs = loadxlstabs(file, tabs_we_want)

    conversionsegments = []
    for tab in tabs:
        junk = tab.excel_ref('A').filter(contains_string('Note')).expand(RIGHT).expand(DOWN)

        category = tab.excel_ref('A').filter(contains_string('Category')).fill(DOWN).is_not_blank()
        category -= junk

        date_time = tab.excel_ref('A').filter(contains_string('Category')).fill(RIGHT).is_not_blank()

        obs = date_time.waffle(category)

        dimensions = [
                HDim(category, 'category', DIRECTLY, LEFT),
                HDim(date_time, 'datetime', DIRECTLY, ABOVE)
                ]

        for cell in dimensions[1].hbagset:
            dimensions[1].AddCellValueOverride(cell, str(cell.value))

        conversionsegment = ConversionSegment(tab, dimensions, obs).topandas()
        conversionsegments.append(conversionsegment)

    df = pd.concat(conversionsegments)

    df['calendar-years'] = df['datetime'].apply(Datetime_To_Years)
    df['Time'] = df['calendar-years']

    df['uk-only'] = 'K02000001'
    df['Geography'] = 'United Kingdom'

    df['dd-mm'] = df['datetime'].apply(Datetime_To_DD_MM)
    df['DayMonth'] = df['dd-mm']

    df['spend-category'] = df['category'].apply(lambda x: x.replace(' ', '-').lower())
    
    df = df.rename(columns={
            'OBS':'v4_0',
            'category':'Category'
            }
        )

    df = df[['v4_0', 'calendar-years', 'Time', 'uk-only', 'Geography',
             'dd-mm', 'DayMonth', 'spend-category', 'Category']]

    df.to_csv(output_file, index=False)
    SparsityFiller(output_file, '.')

def Datetime_To_Years(value):
    # pulls the year from a datetime
    as_datetime = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S') 
    year = datetime.datetime.strftime(as_datetime, '%Y')
    return year

def Datetime_To_DD_MM(value):
    # pulls the date and month from datetime into dd-mm format
    as_datetime = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S') 
    daymonth = datetime.datetime.strftime(as_datetime, '%d-%m')
    return daymonth

# Run transform
if __name__ == '__main__':
    dataset_id = 'uk-spending-on-cards'
    edition = 'time-series'
    collection_name = 'CMD uk spending on cards'
    print(f"Uploading {dataset_id} to CMD")

    upload_dict = {
            dataset_id:{
                    'v4':output_file,
                    'edition':edition,
                    'collection_name':collection_name,
                    'metadata_file':metadata_file
                    }
            }

    Multi_Upload_To_Cmd(credentials, upload_dict)