import json

from pandas import *
import csv


def main():
    #schema
    # userId(Integer), itemId(Integer), tag_id(Integer), title(String), starring(json array), rating(Double), tag(String)

    #read csv file
    data = read_csv('metadata.csv',index_col=False)
    data1 = read_csv('ratings.csv',index_col=False)
    data2 = read_csv('tags.csv',index_col=False)

    #convert starring section to array
    data['starring'] = [(convertArr(str(starring))) for starring in data['starring']]

    #remove unnecessary data from tables
    #remove unnecessary columns
    data.drop(columns=['directedBy', 'starring','dateAdded','avgRating','imdbId'], axis=1, inplace=True)

    #merge the metadata and ratings table on item_id
    #append the columns from tags table onto the combined table
    merged1 = merge(data, data1, on=["item_id"])
    merged2 = concat([merged1, data2],axis=1,join='outer')
    #reorder data
    merged2 = merged2[['user_id', 'item_id', 'id', 'title', 'starring', 'rating','tag']]
    # write to csv file
    merged2.to_csv('result.csv',index=False)

def convertArr(s):
    if type(s) == float:
        return "null"
    else:
        return s.split(',')

if __name__ == '__main__':
    main()
