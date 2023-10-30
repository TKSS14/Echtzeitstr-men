import faust


class Sensor(faust.Record):
    temp_val: float


app = faust.App('Counting_Values', broker='kafka://localhost:9092')
topic = app.topic('temp', value_type=Sensor)

"""
@app.agent(app.topic('temp'))
async def mytask(events):
    async for event in events:
        print(f'test {event}')
        #print(f'Events pro s: {app.monitor.events_s}')
        #print(f'Events gesamt: {app.monitor.events_total}')

"""
@app.agent(topic)
async def count(temperature):
    async for i, value in temperature.enumerate():
        print(f'Eventzähler: {i}')

@app.agent(topic)
async def signal(temperature):
    async for temp in temperature:
        print(f'Temperature: {temp.temp_val}')



app.main()
