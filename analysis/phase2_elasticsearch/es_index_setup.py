import pandas as pd
from elasticsearch import Elasticsearch, helpers

es = Elasticsearch(["http://localhost:9200"])
print("Connected:", es.ping())

# Index mappings
index_mappings = {
    "amazon_reviews": {
        "mappings": {
            "properties": {
                "reviewerID": {"type": "keyword"},
                "asin": {"type": "keyword"},
                "reviewText": {"type": "text"},
                "overall": {"type": "float"},
                "reviewTime": {"type": "date", "format": "yyyy-MM-dd"}
            }
        }
    },
    "amazon_metadata": {
        "mappings": {
            "properties": {
                "asin": {"type": "keyword"},
                "title": {"type": "text"},
                "description": {"type": "text"},
                "price": {"type": "float"},
                "brand": {"type": "keyword"}
            }
        }
    }
}

# Create indices
for index, mapping in index_mappings.items():
    if not es.indices.exists(index=index):
        es.indices.create(index=index, body=mapping)
        print(f"Created index: {index}")

# Load CSVs
reviews_df = pd.read_csv("Amazon_Appliances_Reviews.csv")
metadata_df = pd.read_csv("Amazon_Appliances_Metadata.csv")

reviews_df = reviews_df[["reviewerID", "asin", "reviewText", "overall", "reviewTime"]]
metadata_df = metadata_df[["asin", "title", "description", "price", "brand"]]

reviews_df["reviewTime"] = pd.to_datetime(reviews_df["reviewTime"], errors="coerce").dt.strftime("%Y-%m-%d")

def insert_data(index_name, df):
    bulk_data = [
        {"_index": index_name, "_source": row.dropna().to_dict()}
        for _, row in df.iterrows()
    ]
    helpers.bulk(es, bulk_data)
    print(f"Inserted {len(bulk_data)} docs into {index_name}")

insert_data("amazon_reviews", reviews_df)
insert_data("amazon_metadata", metadata_df)
