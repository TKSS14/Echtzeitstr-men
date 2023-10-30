from abc import ABC

import faust
import numpy as np
import random as rnd
import string


class NumberType(faust.Record):
    temperatur_val: float


class LetterType(faust.Record):
    temperatur_val: str


class CombinedType(faust.Record):
    combined_val: str


app = faust.App('Kombinieren_von_Streams', broker='kafka://localhost:9092')
topic = app.topic('number', value_type=NumberType)
topic2 = app.topic('letter', value_type=LetterType)
topic3 = app.topic('combined', value_type=CombinedType)


@app.agent(topic)
async def number_agent(number):
    async for numb in number:
        print(f'Zahl: {numb.temperatur_val}')


@app.agent(topic2)
async def letters_agent(letter):
    async for let in letter:
        print(f'Buchstabe: {let.temperatur_val}')


@app.agent(topic3)
async def combined_agent(combined):
    async for comb in combined:
        print(f'Combined: {comb.combined_val}')


@app.timer(interval=1.0)
async def number_sender(app):
    number = rnd.uniform(1, 100)
    await number_agent.send(
        value=NumberType(temperatur_val=number),
    )


@app.timer(interval=1.0)
async def letter_sender(app):
    letter = rnd.choice(string.ascii_letters)
    await letters_agent.send(
        value=LetterType(temperatur_val=letter),
    )


@app.agent(app.topic('number', 'letter'))
async def mytask(value):
    async for val in value:
        await combined_agent.send(
            value=CombinedType(combined_val=val.temperatur_val),
        )


app.main()
