import requests
import json

url = "https://search-healthos-es67-nocrz73cfktuhhkgnaz72td3cq.us-east-1.es.amazonaws.com/products104/_search"

payload = json.dumps({
    "query": {
        "terms": {
            "sku": [
                "6630340696267782",
                "6630340696267742",
                "6630340696267739",
                "6630340696267736",
                "6630340696267733",
                "36856120585267705",
                "33258524072881231",
                "33258524072881230",
                 "39690134756841650",
                "33258524072881227",
                "33258524072881225",
                "33258524072881224",
                "33258524072881222",
                "33258524072881221"
            ]
        }
    }
})
headers = {
    'Authorization': 'Basic cHJvZHVjdHMxMDQ6UHJvZHVjdHMxMDRA',
    'Content-Type': 'application/json'
}

response = requests.request("GET", url, headers=headers, data=payload)

# print(response)
json_data = response.json()
hits = json_data['hits']['hits']
rows = []
# print(len(hits['hits']))
for k in hits:
    try:
        i = k.get('_source')
        name = i.get('name')
        brandName = i.get('brand').get('name')
        url = i.get('url')
        class1 = i.get('classification').get('l1')
        regPrice = i.get('price').get('regular_price').get('value')
        offPrice = i.get('price').get('offer_price').get('value')
        stock = i.get('stock').get('available')
        knn_items = i['similar_products']['website_results']['62037a37110b3f66c0238d5a']['knn_items']
        # print(len(matchesProd))
        if len(knn_items) == 0:
            continue
        matchesProd = knn_items[0]['_source']
        matchesProdName = matchesProd['name']
        matchesProdBrand = matchesProd['brand']['name']
        matchesProdURL = matchesProd['url']
        matchesProdClass = matchesProd['classification']['l1']
        matchesProdRegPrice = matchesProd['price']['regular_price']['value']
        matchesProdOffPrice = matchesProd['price']['offer_price']['value']
        matchesProdStock = matchesProd['stock']['available']
        row = [name, brandName, url, class1, regPrice, offPrice, stock,
               matchesProdName, matchesProdBrand
            , matchesProdURL, matchesProdClass, matchesProdRegPrice, matchesProdOffPrice, matchesProdStock]
        print(row)
        rows.append(row)
    except KeyError:
        pass

