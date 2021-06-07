import os
import asyncio
from collections import namedtuple
import matplotlib.pyplot as plt
import faust
from faust.cli import option
from streamad.models import Input, Output
from streamad.utils import parse_csv_data


DataPoint = namedtuple('DataPoint', 'x y anomaly')
_GRAPH_COMMIT_INTERVAL_SEC = 60
_MODELS = {}

app = faust.App(
    'streamad-test',
    broker='kafka://' + os.getenv('KAFKA_BROKER', 'localhost:9092')
)
input_topic = app.topic(
    'streamad-input', value_type=Input)
output_topic = app.topic(
    'streamad-output', value_type=Output)


@app.command(
    option('--path',
           type=str, default=None,
           help='Path with csv data to be produced'),
)
async def produce(self, path) -> None:
    if path is None:
        self.say("You must provide the --path option")

    async def produce_values(model_id, values):
        async for value in values:
            await input_topic.send(value=value, key=model_id)

    files = filter(lambda x: x.endswith('.csv'), os.listdir(path))
    produce_args = ((fn.lower(), os.path.join(path, fn)) for fn in files)

    await asyncio.gather(*[
        produce_values(model_id, parse_csv_data(filepath, model_id))
        for model_id, filepath in produce_args
    ])


@app.agent(output_topic)
async def graph_output(output_stream):
    async for row in output_stream:
        try:
            data = _MODELS[row.model_id]
        except KeyError:
            data = []
            _MODELS[row.model_id] = data

        anomaly_score = row.anomaly_score if row.anomaly_score > 0.5 else 0
        data.append(DataPoint(row.ts, row.value, row.anomaly_score))


@app.timer(interval=_GRAPH_COMMIT_INTERVAL_SEC)
async def graph_commit():
    print('graph commit')
    for model_id in _MODELS:
        data = _MODELS[model_id]
        data.sort(key=lambda p: p.x)

        x = [point.x for point in data]
        y = [point.y for point in data]
        anomaly = [point.anomaly for point in data]

        plt.figure()
        fig, ax1 = plt.subplots()

        color = 'tab:blue'
        ax1.set_xlabel('time')
        ax1.set_ylabel('metric', color=color)
        ax1.plot(x, y, color=color)
        ax1.tick_params(axis='y', labelcolor=color)

        ax2 = ax1.twinx()
        color = 'tab:red'
        ax2.set_ylabel('anomaly', color=color)
        ax2.plot(x, anomaly, '.', color=color)
        ax2.tick_params(axis='y', labelcolor=color)

        fig.tight_layout()
        plt.savefig(f'debug/{model_id}.svg', format='svg', dpi=1200)
        plt.close()


if __name__ == '__main__':
    app.main()