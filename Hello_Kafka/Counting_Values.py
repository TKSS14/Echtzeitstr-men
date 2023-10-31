import faust
import random as rnd


class Sensor(faust.Record):
    temp_val: float


app = faust.App('Counting_Values', broker='kafka://localhost:9092')
topic = app.topic('temp', value_type=Sensor)


@app.timer(interval=1.0)
async def example_sender(app):
    temperatur = rnd.uniform(20, 30)
    await topic.send(
        value=Sensor(temp_val=temperatur),
    )


app.main()
