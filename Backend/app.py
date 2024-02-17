import datetime
from typing import Union
from fastapi import FastAPI
from EVENT_parser.GEOSPLIT import create_post

app = FastAPI()


@app.get('/')
def hello():
    return {'Hello': 'World'}


@app.get('/event/')
def hello(name: str, surname: str, link: str, gpxfile: Union[str, None] = '', start: Union[str, None] = ''):
    # return {'name': name, 'surname': surname, 'link': link}
    start = datetime.datetime.now()
    post = create_post(name, surname, link, gpxfile=gpxfile, start=start)
    fin = datetime.datetime.now() - start
    post.setdefault('elapse time', fin)
    return post

