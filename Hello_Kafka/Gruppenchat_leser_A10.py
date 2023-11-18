import faust


class Chat(faust.Record):
    message: str


app = faust.App('Gruppenchat', broker='kafka://localhost:9092')
topic = app.topic('chatroom', value_type=Chat)


@app.agent(topic)
async def signal(chat):
    async for msg in chat:
        print(f'Message: {msg.message}')

app.main()
