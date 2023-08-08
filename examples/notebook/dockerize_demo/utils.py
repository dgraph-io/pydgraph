import os
import random
import graphistry

from IPython.display import Markdown, display

def warning(text):
    return Markdown(f'<div class="alert alert-danger">{text}</div>')

def graphistry_login():
    servers = ['3.232.235.63']
    gserver = servers[random.randint(0, len(servers) - 1)]
    graphistry.register(api=3, server=gserver, protocol='http', username='kgc-user', password=os.getenv("GRAPHISTRY_PASSWORD")) 
    print("Graphistry login successful, version: ", graphistry.__version__)
    
