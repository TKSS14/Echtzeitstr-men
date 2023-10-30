import faust
import random as rnd


class Sensor(faust.Record):
    temp_val: float


app = faust.App('Filterung_und_Weiterleitung', broker='kafka://localhost:9092')
topic = app.topic('temp', value_type=Sensor)
topic2 = app.topic('hightemp', value_type=Sensor)
echo1 = app.topic('echo1', value_type=Sensor)
echo2 = app.topic('echo2', value_type=Sensor)


@app.agent(topic)
async def signal(temperature):
    async for temp in temperature:
        print(f'Temperature: {temp.temp_val}')


@app.agent(topic2)
async def signal2(temperature):
    async for temp in temperature:
        print(f'High Temperature: {temp.temp_val}')


@app.agent(echo1)
async def signal3(temperature):
    async for temp in temperature:
        print(f'Echo1: {temp.temp_val}')
        print(temperature)


@app.agent(echo2)
async def signal4(temperature):
    async for temp in temperature:
        print(f'Echo2: {temp.temp_val}')
        print(temperature)


@app.timer(interval=1.0)
async def example_sender(app):
    temperatur = rnd.uniform(20, 30)
    await signal.send(
        value=Sensor(temp_val=temperatur),
    )


@app.agent(topic)
async def filter_temp(temperature):
    async for temp in temperature.filter(lambda v: v.temp_val > 25):
        await signal2.send(value=Sensor(temp_val=temp.temp_val))


@app.agent(topic2)
async def process(stream):
    async for event in stream.echo(echo1, echo2):
        print("Echoing")


app.main()
