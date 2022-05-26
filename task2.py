from fastapi import FastAPI, File, UploadFile
import pandas as pd
from fastapi.responses import StreamingResponse
import io
import requests
import json

app = FastAPI()


@app.post("/get_matched_products")
async def upload1(file: UploadFile = File(...)):
    df = pd.read_csv(file.file)
    url = "ELASTIC-SEARCH-URL"
    l = df['SKU'].to_numpy().tolist()
    batches = len(l) / 10
    startIndex = 0
    endIndex = 10
    rows = []
    for i in range(0, int(batches)):
        currentBatch = l[startIndex:endIndex]
        payload = json.dumps({
            "query": {
                "terms": {
                    "sku": currentBatch
                }
            }
        })
        headers = {
            'Authorization': 'Basic cHJvZHVjdHMxMDQ6UHJvZHVjdHMxMDRA',
            'Content-Type': 'application/json'
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        try:
            json_data = response.json()
            hits = json_data['hits']['hits']
            print(len(hits))
            # print(len(hits['hits']))
            for k in hits:
                try:
                    i = k.get('_source')

                    name = i.get('name')
                    brandName = i.get('brand').get('name')
                    regPrice = i.get('price').get('regular_price').get('value')
                    offPrice = i.get('price').get('offer_price').get('value')

                    knn_items1 = i['similar_products']['website_results']['618a5fcb2324f3ad279b24dc']['knn_items']
                    knn_items2 = i['similar_products']['website_results']['62037a37110b3f66c0238d5a']['knn_items']

                    if len(knn_items1) == 0:
                        continue
                    if len(knn_items2) == 0:
                        continue

                    ssense = knn_items1[0]['_source']
                    ssenseProdName = ssense['name']
                    ssenseProdBrand = ssense['brand']['name']
                    ssenseProdRegPrice = ssense['price']['regular_price']['value']
                    ssenseProdOffPrice = ssense['price']['offer_price']['value']

                    farfetch = knn_items2[0]['_source']
                    farfetchProdName = farfetch['name']
                    farfetchProdBrand = farfetch['brand']['name']
                    farfetchProdRegPrice = farfetch['price']['regular_price']['value']
                    farfetchProdOffPrice = farfetch['price']['offer_price']['value']

                    row = [name, brandName, regPrice, offPrice,
                           ssenseProdName, ssenseProdBrand, ssenseProdRegPrice, ssenseProdOffPrice,
                           farfetchProdName, farfetchProdBrand, farfetchProdRegPrice, farfetchProdOffPrice]
                    print("ROW", row)
                    rows.append(row)
                except KeyError:
                    pass
            startIndex += 10
            endIndex += 10
        except ValueError:
            pass

    task1_op_cols = createTask2Cols()
    stream = io.StringIO()
    op_df = pd.DataFrame(rows, columns=task1_op_cols)
    op_df.to_csv(stream, index=False)
    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=export.csv"
    return response


# return response


def createTask2Cols():
    cols = ["Net-a-porter Product Name"
        , "Net-a-porter Product Brand"
        , "Net-a-porter Regular Price"
        , "Net-a-porter Offer Price"
        , "Ssense Product Name"
        , "Ssense Product Brand"
        , "Ssense Regular Price"
        , "Ssense Offer Price"
        , "Farfetch Product Name"
        , "Farfetch Product Brand"
        , " Farfetch Regular Price"
        , "Farfetch Offer Price"]
    return cols

# print(createDataFrame())
