import logging
import sys
import configparser
import boto3
from botocore.exceptions import ClientError



def upload_to_s3(filepath, destination):
    '''
    Takes the local filepath and S3 key for the file to upload
    
    Inputs:
    - filepath: the filepath in your local directory
    - destination: S3 key for the file we want to upload
    
    Outputs:
    - NA
    '''
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    BUCKET                 = config.get('S3', 'BUCKET_NAME')
    
    s3 = boto3.resource('s3', region_name='eu-west-2',
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET)
    
    s3.meta.client.upload_file(Filename=filepath, Bucket=BUCKET, Key=destination)

def main():
    '''
    Function that is called when the file is called directly
    '''
    
    
    if len(sys.argv) == 5:

        pp_filepath, pp_key, postcodes_filepath, postcodes_key = sys.argv[1:]
        
        print('Uploading postcodes ...')
        upload_to_s3(postcodes_filepath, postcodes_key)
        
        print('Uploading prices paid data ...')
        upload_to_s3(pp_filepath, pp_key)
        
        print('\nSuccessfully uploaded!\n')
        
    else:
        print('In order, please provide: the filepath of the price paid data, '\
              'its desired name in the S3 bucket, the filepath of the postcodes data, '\
              'its desired name in the S3 bucket. \n\nExample: python upload_data.py '\
              'pp-complete.csv prices_paid postcodes.csv postcodes')

if __name__=="__main__":
    main()