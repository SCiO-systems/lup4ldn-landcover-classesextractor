import numpy as np
import numpy.ma as ma
import gdal
import os
import boto3
from botocore.exceptions import ClientError
import json
import logging

s3 = boto3.client('s3')



def lambda_handler(event, context):
    
    body = json.loads(event['body'])
    json_file = body
    
    #get  input json and extract geojson
    try:
        project_id = json_file["project_id"]
        ROI = json_file["ROI"]
        if ROI==None:
            ROI = requests.get(json_file["ROI_file_url"])
            ROI = json.loads(ROI.text) #.replace("'",'"')
        if "land_use_map" in json_file:
            if json_file["land_use_map"]["custom_map_url"]!="n/a":
                default=False
            else:
                default=True
        else:
            default=True
        
    except Exception as e:
        print("Input JSON field have an error.")
        return {
            "statusCode": 400,
            "body": e
        }
    

    #for aws
    path_to_tmp = "/tmp/"
    s3_lambda_path = '/vsis3/lup4ldn-prod/'
    
    gdal_warp_kwargs_target_area = {
        'format': 'GTiff',
        'cutlineDSName' : json.dumps(ROI),
        'cropToCutline' : True,
        'height' : None,
        'width' : None,
        'srcNodata' : -32768.0,
        'dstNodata' : -32768.0,
        'creationOptions' : ['COMPRESS=DEFLATE']
    }
    
    ## DEFAULT CASE: CLASSES from land cover
    if default:
        save_land_cover_file = path_to_tmp + "cropped_land_cover.tif"
    
        try:
            #CHANGE HERE THE YEAR IF MORE YEARS ARE TO BE USED
            gdal.Warp(save_land_cover_file,s3_lambda_path + project_id + "/cropped_land_cover.tif",**gdal_warp_kwargs_target_area)
        except Exception as e:
            print("if 'returned NULL without setting an error', probably at least one of the file paths is wrong")
            return {
                "statusCode": 500,
                "body": e
            }
        
        #must use gdal.Open in order to fill the file created from gdal.Warp, else the file remaines full of nodata
        try:
            land_cover_tif = gdal.Open(save_land_cover_file)
            land_cover_array = land_cover_tif.ReadAsArray()
        except Exception as e:
            print("if ''NoneType' object has no attribute', probably the file path is wrong")
            raise(e)
        
        
        def map_land_cover_to_trendsearth_labels(array,labels_dict):
            for key in labels_dict:
                array = np.where(array==key,labels_dict[key],array)
            return array
        dict_labels_map_100m_to_trends = {
            10 : 3,
            11 : 3,
            12 : 3,
            20 : 3,
            30 : 3,
            40 : 2,
            50 : 1,
            60 : 1,
            61 : 1,
            62 : 1,
            70 : 1,
            71 : 1,
            72 : 1,
            80 : 1,
            81 : 1,
            82 : 1,
            90 : 1,
            100 : 1,
            110 : 2,
            120 : 2,
            121 : 2,
            122 : 2,
            130 : 2,
            140 : 2,
            150 : 2,
            151 : 2,
            152 : 2,
            153 : 2,
            160 : 4,
            170 : 4,
            180 : 4,
            190 : 5,
            200 : 6,
            201 : 6,
            202 : 6,
            210 : 7,
            220 : 6,
            0 : -32768
        }
        
        land_cover_array = map_land_cover_to_trendsearth_labels(land_cover_array,dict_labels_map_100m_to_trends)
        unique, counts = np.unique(land_cover_array, return_counts = True)
        if -32768 in unique:
            unique = unique[1:]
            counts = counts[1:]
        lc_hectares = dict(zip([str(x) for x in unique],  [9 * int(x) for x in counts]))
    
    ## CUSTOM CASE: CLASSES from land use
    else:
        save_land_use_file = path_to_tmp + "cropped_land_use.tif"
        
        def create_vsis3_url(url):
            part1 = url.split(".s3.")[0]
            part2 = url.split(".amazonaws.com")[1]
            vsis3_url = (part1+part2).replace("https:/","/vsis3" )
            return vsis3_url
            
        s3_lambda_path = '/vsis3/lup4ldn-prod/'
        try:
            gdal.Warp(save_land_use_file,create_vsis3_url(json_file["land_use_map"]["custom_map_url"]),**gdal_warp_kwargs_target_area)
        except Exception as e:
            print("if 'returned NULL without setting an error', probably at least one of the file paths is wrong")
            return {
                "statusCode": 500,
                "body": e
            }
        
        #must use gdal.Open in order to fill the file created from gdal.Warp, else the file remaines full of nodata
        try:
            t = gdal.Open(save_land_use_file)
        except Exception as e:
            print("if ''NoneType' object has no attribute', probably the file path is wrong")
            return {
                "statusCode": 500,
                "body": e
            }
            
        land_use_array = t.ReadAsArray()
        land_use_array = np.where(land_use_array<=0, -32768,land_use_array)
        
        unique, counts = np.unique(land_use_array, return_counts = True)
        if -32768 in unique:
            unique = unique[1:]
            counts = counts[1:]
        
        lc_hectares = dict(zip([str(x) for x in unique],  [9*int(x) for x in counts]))

    

    my_output = {
    "land_cover_hectares_per_class" : lc_hectares
    }
    
    
    return {
        "statusCode": 200,
        "body": json.dumps(my_output)
    }
    