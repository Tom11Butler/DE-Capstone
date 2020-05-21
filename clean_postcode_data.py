import sys
import pandas as pd

def process_data(filepath):
    '''
    Want this to take arguments from the console input, so we tell it the filepath of the data we want to send
    Think along the lines of the 
    '''
    
    print('Cleaning the data ...')
    
    postcodes = pd.read_csv(filepath)
    
    area_classes = postcodes['Output Area Classification Name'].str.split(';', expand=True)

    postcodes['Area_Class_1'] = area_classes[0]
    postcodes['Area_Class_2'] = area_classes[1]
    postcodes['Area_Class_3'] = area_classes[2]

    postcodes.drop(labels=['Output Area Classification Name'], axis=1, inplace=True)
    
    # only want to keep the useful features
    # (this was done after looking at which ones are worth keeping for our purposes)

    useful_features = ['Postcode 1'
                       , 'Postcode 2'
                       , 'Postcode 3'
                       , 'County Name'
                       , 'Local Authority Name'
                       , 'Ward Name'
                       , 'Country Name'
                       , 'Region Name'
                       , 'Parliamentary Constituency Name'
                       , 'European Electoral Region Name'
                       , 'Primary Care Trust Name'
                       , 'Lower Super Output Area Name'
                       , 'Middle Super Output Area Name'
                       , 'Longitude'
                       , 'Latitude'
                       , 'Last Uploaded'
                       , 'Location'
                       , 'Area_Class_1'
                       , 'Area_Class_2'
                       , 'Area_Class_3']

    postcodes = postcodes[useful_features]
    
    print('Writing the cleaned file to a csv ...')
    
    postcodes.to_csv(path_or_buf=filepath, index=False)

    
def main():
    '''
    Cotains main functions of the program
    
    Included the template to follow for the system arguments
    '''
    
    
    if len(sys.argv) == 2:

        postcodes_filepath = sys.argv[1:][0]
        
        process_data(postcodes_filepath)
        
        print('\n Successfully cleaned! \n')
        
    else:
        print('Please provide the filepath of the postcodes dataset to clean.')


if __name__=="__main__":
    main()