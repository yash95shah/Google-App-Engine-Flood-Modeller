from google.cloud import datastore
import os
project_id = os.getenv('GCLOUD_PROJECT')
client = datastore.Client(project_id)

def inject_into_datastore(data, name=None):
    key =client.key('dataduplicate', name)
    j_entity = datastore.Entity(key=key)
    for j_prop, j_val in data.items():
        j_entity[j_prop] = j_val
    client.put(j_entity)
    return 'Job finished'

def query_fetch(collection, record ):
    print ("Entered into it")
    key_to_get = client.key(collection, record)
    val = client.get(key_to_get)
    return val


if __name__ == '__main__':
    water_data = query_fetch('data','is_water')
  
    inject_into_datastore(water_data,'test_data')

