import asyncio
import aiohttp
import pandas as pd


API_KEY = 'qI5lW8HBQSF3JNZHnWvbR02auviARIbwhhU1Kl6d'
url = 'https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/?frequency=hourly&data[0]=value&facets[fueltype][]=WND&facets[respondent][]=AECI&facets[respondent][]=AVA&facets[respondent][]=AVRN&facets[respondent][]=AZPS&facets[respondent][]=BPAT&facets[respondent][]=CHPD&facets[respondent][]=CISO&facets[respondent][]=ERCO&facets[respondent][]=GWA&facets[respondent][]=IPCO&facets[respondent][]=ISNE&facets[respondent][]=LDWP&facets[respondent][]=MISO&facets[respondent][]=NEVP&facets[respondent][]=NWMT&facets[respondent][]=NYIS&facets[respondent][]=PACE&facets[respondent][]=PACW&facets[respondent][]=PJM&facets[respondent][]=PNM&facets[respondent][]=PSCO&facets[respondent][]=PSEI&facets[respondent][]=SOCO&facets[respondent][]=SRP&facets[respondent][]=SWPP&facets[respondent][]=TEPC&facets[respondent][]=TVA&facets[respondent][]=WACM&facets[respondent][]=WALC&facets[respondent][]=WWA&start=2018-07-01T00&end=2018-12-31T00&sort[0][column]=period&sort[0][direction]=asc&offset={}&length={}&api_key={}'
header = {
    'content_type':'application/json'
}
results = []
csv_filename = 'eia....'
num_of_records = 24000
number_of_iterations = int(num_of_records/5000) + 1


async def get_records():
    offset = 0
    length = 5000
    i = 0
    async with aiohttp.ClientSession() as session:
        while i <= number_of_iterations:
            await asyncio.sleep(1)
            response = await asyncio.create_task(session.get(url.format(offset, length,API_KEY), headers=header, ssl=False))
            results.append(await response.json())
            print(offset)
            offset = offset + 5000
            length = length + 5000
            i += 1

asyncio.run(get_records())


main = pd.DataFrame()
for result in results:
    dict_value = result['response']['data']
    df = pd.DataFrame(dict_value) 
    main = pd.concat([main,df], axis=0) 

main.to_csv("{csv_filename}.csv",index=False)