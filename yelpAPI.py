import requests
import boto3
import json
from datetime import datetime


def api_pull_restaurants(loc, cuisine):

    URL = "https://api.yelp.com/v3/businesses/search"
    API_KEY = '7VzkWetGL10gdf9-eCmkUaNbnFSMMHd09O3lal_l2nzVDVDp-XcWwteEzQRvwgI0gU9dF9lvh0aE3ql-KULTgpXXUixAmdjw34DWSizC-HX1WnJuwXakh-eMuD31Y3Yx'

    headers = {
        'Authorization': f"Bearer {API_KEY}"
    }

    all_businesses = []
    keys = []

    ########################################################
    ### make sure to change the range to 20 instead of 2 ###
    ########################################################
    for i in range(21):
        params = {
            'location': loc,
            'term': cuisine + ' restaurants',
            'limit': 50,
            'offset': 50*i}
        response = requests.get(url=URL, params=params, headers=headers)
        
        # print(response.url)
        # print(response.status_code)

        print(i)

        yelp_data = response.json()
        if 'businesses' in yelp_data.keys():
            all_businesses += yelp_data['businesses']

    temp = set()
    removes=[]
    for i in range(len(all_businesses)):
        if all_businesses[i]['name'] in temp:
            removes.append(i)
        else:
            temp.add(all_businesses[i]['name'])

    removes.sort()
    for i in removes[::-1]:
        all_businesses.pop(i)

    return all_businesses

def put_dynamo_db(sample_item):

    dynamodb = boto3.client('dynamodb')

    # REQUIREMENTS: Business ID, Name, Address, Coordinates, Number of Reviews, Rating, Zip Code

    fields = ['id', 'name', 'coordinates', 'location', 'review_count', 'rating']
    open_fields = ['location', 'coordinates']
    loc_fields = ['zip_code', 'display_address']
    coord_fields = ['latitude', 'longitude']

    map_fields = {'location': loc_fields, 'coordinates': coord_fields}
    map_types = {'id': 'S', 'name': 'S', 'latitude': 'S', 'longitude': 'S', 'zip_code': 'S', 'display_address': 'S', 'review_count': 'S', 'rating': 'S'}

    new_item = {}

    for key, value in sample_item.items():
        if key in fields:
            if key in open_fields:
                l = map_fields[key]
                for subcat in l:
                    if subcat == 'display_address':
                        combined_value = ' '.join(value[subcat])
                        new_item[subcat] = {map_types[subcat]: combined_value}
                    else:
                        new_item[subcat] = {map_types[subcat]: str(value[subcat])}
            else:
                new_item[key] = {map_types[key]: str(value)}

    # print(new_item)

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    new_item['insertedAtTimestamp'] = {'S': str(timestamp)}
    dynamodb.put_item(TableName='yelp-restaurants', Item = new_item)

    return print("Put " + new_item['name']['S'] + " successful. \n")


def main():
    cuisines = ['japanese', 'chinese', 'italian', 'american', 'korean', 'mexican', 'british']
    location = 'Manhattan'

    shift = 0

    for cuisine in cuisines:
        all_businesses = api_pull_restaurants(location, cuisine)

        for b in all_businesses:
            put_dynamo_db(b)
        
        elastic = []

        shift = shift + len(all_businesses)

        for b in range(len(all_businesses)):
            elastic.append({'index': {'_index': 'restaurants', '_id': shift+b}})
            elastic.append({'RestaurantID': all_businesses[b]['id'], 'cuisine': cuisine})

        with open("sample.json", "a") as outfile:
            for e in elastic:
                json.dump(e, outfile)
                outfile.write('\n')


if __name__ == "__main__":
  
    main()