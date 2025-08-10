from queryParser import QueryParser
import pickle

parser = QueryParser()
with open("models/query_parser.pkl", "wb") as f:
    pickle.dump(parser, f)