import faust


class Chat(faust.Record):
    message: str


app = faust.App('Gruppenchat', broker='kafka://localhost:9092')
topic = app.topic('chatroom', value_type=Chat)


@app.timer(interval=0.5)
async def example_sender(app):
    msg = input('Write your message:\n')
    await topic.send(value=Chat(message=msg))

app.main()
