import pandas as pd
import asyncio
import aiohttp
from functools import reduce
from datetime import datetime
from dateutil import tz
import pytz
import mpld3

# raw ='CAR', 'CENT', 'FLA', 'MIDA', 'MIDW', 'NE', 'NY', 'NW', 'SE', 'SW',
regions = ['CAL', 'TEN', 'TEX', 'CENT', ]
api_key = 'Gg24TpEKJrrcywG1cQlZxq4hPrln5uzu6YJaqtCK'
urls = [f'https://api.eia.gov/series/?api_key={api_key}&series_id=EBA.{region}-ALL.NG.NUC.H&periods=14H' for region
        in
        regions]


async def get_data_from_one_url(session, url):
    async with session.get(url) as r:
        return await r.json()


async def get_all(session, urls):
    tasks = []
    for url in urls:
        task = asyncio.create_task(get_data_from_one_url(session, url))
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    return results


async def main(urls):
    async with aiohttp.ClientSession() as session:
        data = await get_all(session, urls)
        return data


#
def help_string(string):
    modified_str = (string[:4] + '-' + string[4:6] + '-' + string[6:8]) + (string[8:].replace('T', ' ')).replace('Z',
                                                                                                                 ':') + '00:00'
    utc = datetime.strptime(modified_str, '%Y-%m-%d %H:%M:%S')
    utc = utc.replace(tzinfo=pytz.UTC)
    local = utc.astimezone(tz.tzlocal())
    result = local.strftime('%Y-%m-%d %H:%M:%S')
    return result


def data_transformer(results):
    frames = []
    for result in results:
        data = result['series'][0]['data']
        data_for_frame = {
            "time": [help_string(i[0]) for i in data],
            f"{regions[0]}": [i[1] for i in data]
        }
        frames.append(data_for_frame)
        regions.remove(regions[0])
    frame_list = [pd.DataFrame(i) for i in frames]
    df_final = reduce(lambda left, right: pd.merge(left, right, on='time'), frame_list)
    df_final.to_csv("example.csv", index=False)
    df_final.to_html("example.html")
    return frame_list


def plot_generator(frame_list):
    figures = []
    for frame in frame_list:
        plot = frame.plot()
        figure = plot.get_figure()
        figures.append(figure)
    with open('../../example.html', 'a') as f:
        f.write(mpld3.fig_to_html(figures[0], d3_url=None, mpld3_url=None, no_extras=False, template_type='general',
                                  figid=None, use_http=False, include_libraries=True))
        f.write(mpld3.fig_to_html(figures[1], d3_url=None, mpld3_url=None,
                                  no_extras=False, template_type='general', figid=None, use_http=False,
                                  include_libraries=True))
        f.write(mpld3.fig_to_html(figures[2], d3_url=None, mpld3_url=None,
                                  no_extras=False, template_type='general', figid=None, use_http=False,
                                  include_libraries=True))

    return figures


if __name__ == "__main__":
    results = asyncio.run(main(urls))
    data = data_transformer(results)
    plot_generator(data)
