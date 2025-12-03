from elasticsearch import Elasticsearch

es = Elasticsearch(["http://localhost:9200"])

# Query 1
query1 = {
    "query": {
        "bool": {
            "must": [{"match": {"reviewText": "easy"}}],
            "must_not": [{"match": {"reviewText": "again"}}]
        }
    },
    "size": 5
}

# Query 2
query2 = {
    "query": {
        "bool": {
            "must": [{"match": {"reviewText": {"query": "refrigirator", "fuzziness": "AUTO"}}}],
            "filter": [{"range": {"overall": {"lte": 3.5}}}]
        }
    },
    "size": 5
}

# Query 3
query3 = {
    "query": {
        "span_near": {
            "clauses": [
                {"span_term": {"reviewText": "bad quality"}},
                {"span_term": {"reviewText": "expensive"}}
            ],
            "slop": 5,
            "in_order": False
        }
    },
    "size": 5
}

# Query 4
query4 = {
    "query": {
        "bool": {
            "must": [{"match": {"reviewText": "overheating dishwasher"}}],
            "must_not": [{"match": {"reviewText": "overheating dryer"}}]
        }
    },
    "size": 5
}

# Query 5
query5 = {
    "query": {
        "bool": {
            "must": [
                {"match": {"reviewText": {"query": "overheaitng dishwasher", "fuzziness": "AUTO"}}},
                {"match": {"reviewText": {"query": "overheating dreyr", "fuzziness": "AUTO"}}}
            ]
        }
    },
    "size": 5
}

# Query 6
query6 = {
    "query": {
        "bool": {
            "must": [
                {"match": {"reviewText": "liked the product"}},
                {"match": {"reviewText": "expensive"}}
            ]
        }
    },
    "size": 5
}

# Query 7
query7 = {
    "query": {
        "bool": {
            "must": [
                {"match": {"reviewText": "ice maker problem"}},
                {"match": {"reviewText": "French doors"}}
            ]
        }
    },
    "size": 5
}

queries = [
    ("Query 1: easy but NOT again", query1),
    ("Query 2: misspelled Refrigerator + ≤3.5", query2),
    ("Query 3: bad quality near expensive", query3),
    ("Query 4: overheating dishwasher NOT dryer", query4),
    ("Query 5: fuzzy overheating dishwasher & dryer", query5),
    ("Query 6: liked product but expensive", query6),
    ("Query 7: ice maker + French doors", query7),
]

for label, q in queries:
    print(f"\n--- {label} ---")
    res = es.search(index="amazon_reviews", body=q)
    for hit in res["hits"]["hits"]:
        print(hit["_source"])
