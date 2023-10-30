import faust
import numpy as np
import random as rnd


class Sensor(faust.Record):
    temp_val: float


app = faust.App('Signalverarbeitung', broker='kafka://localhost:9092')
topic = app.topic('sensor', value_type=Sensor)


@app.agent(topic)
async def signal(temperature):
    async for temp in temperature:
        print(f'Temperature: {temp.temp_val}')


@app.timer(interval=1.0)
async def example_sender(app):
    temperatur = rnd.uniform(20, 30)
    await signal.send(
        value=Sensor(temp_val=temperatur),
    )


@app.agent(topic)
async def mean(temperature):
    async for values in temperature.take(100, within=10):
        temp_mean = np.array([])
        for temp in values:
            temp_mean = np.append(temp_mean, temp.temp_val)
        print(f'The mean temperature is : {np.mean(temp_mean)}')

app.main()
