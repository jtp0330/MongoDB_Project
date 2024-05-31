import pandas as pds
import csv
import json

def main():
    citylistConvert()





def citylistConvert():
    data = pds.read_json('zips.json',lines=True)
    data.set_axis(['zip','city','loc','pop','state'],axis=1,inplace=True)
    data = data[['state','city', 'zip','loc','pop']]
    result = data.to_csv('new_zips.csv',index=False)
    print(result)






#main method
if __name__ == '__main__':
    main()