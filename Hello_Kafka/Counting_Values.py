import faust
import random as rnd


class Sensor(faust.Record):
    temp_val: float


app = faust.App('Counting_Values', broker='kafka://localhost:9092')
topic = app.topic('temp', value_type=Sensor)


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
async def count(temperature):
    async for i, value in temperature.enumerate():
        print(f'Eventzähler: {i}')


app.main()
