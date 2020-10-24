import tempfile
from webdav3.client import Client
from PIL import Image
from PIL.ExifTags import TAGS
from elasticsearch import Elasticsearch
import json
import os
import iptcinfo3

def iterate(client , path, es:Elasticsearch):
    test = client.list(path,get_info=True)
    test.pop(0)
    for file in test:
        if file['isdir']:
            path_extension = clean_path(file['path'])
            iterate(client,path + "/" + path_extension, es)
        else:
            download_image(client,path + "/" + file['path'].split("/").pop(),es)

def clean_path(path: str) -> str :
    splitted_path = path.split("/")
    return splitted_path[len(splitted_path)-2]

def download_image(client, path:str, es:Elasticsearch):
    with tempfile.TemporaryDirectory() as tmpdirname:
        path_local = tmpdirname + "/" + path.split("/").pop()
        client.download_sync(remote_path=path, local_path=path_local)
        keywords(path_local, es)

def keywords(path:str, es:Elasticsearch):
    info = iptcinfo3.IPTCInfo(path)
    listKeyword = []
    for keyword in info['keywords']:
        if isinstance(keyword,bytes):
            listKeyword.append(keyword.decode())
            print(keyword.decode())
        else:
            #print(keyword)
            listKeyword.append(keyword)
    print(listKeyword)
    mapItems = {}
    mapItems["keyword"] = listKeyword
    with Image.open(path) as image:
        exifdata = image.getexif()
        for tag_id in exifdata:
            #get the tag name, instead of human unreadable tag id
            tag = TAGS.get(tag_id, tag_id)
            data = exifdata.get(tag_id)
            #decode bytes 
            if isinstance(data, bytes):
                data = data.decode()
            #print(f"{tag:25}: {data}")
            mapItems[tag] = str(data)
    res = es.index(index="test-image", body=mapItems)
    

es = Elasticsearch()
with open('config.json') as json_file:
    options = json.load(json_file)
    client = Client(options)
    path = "Bilder/1996"
    #download_image(None,"tets")
    iterate(client, path, es)
es.indices.refresh(index="test-image")



