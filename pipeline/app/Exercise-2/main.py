import requests
import pandas as pd
from bs4 import BeautifulSoup
import os

download_path = os.path.join(os.path.dirname(__file__), 'downloads')

def make_download_dir(path=download_path):
    try:
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
    except:
        print('Lỗi khi tạo file "downloads"')


def download_file(url):
    f_name = url.split('/')[7]
    save_dir = os.path.join(download_path, f_name) # Creating save directory

    try:
        resp = requests.get(url, stream=True)

        with open(save_dir, 'wb') as f:
            for content in resp.iter_content(chunk_size=8192):
                f.write(content)
    except:
        print('Loi trong qua trinh tai va luu file')


def highest_HourlyDryBulbTemperature(target='HourlyDryBulbTemperature'):
    for fname in os.listdir(download_path):
        if fname.endswith('.csv'):
            csv_dir = os.path.join(download_path, fname)
            df = pd.read_csv(csv_dir)
            return max(df[target])



def main(url='https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'):
    resp = requests.get(url).text
    soup = BeautifulSoup(resp, 'html.parser')
    records = soup.find_all('tr')[3:]
    for record in records:
        tds = record.find_all('td')
        if tds[1].get_text().strip() == '2024-01-19 10:27':
            url_down = url + tds[0].get_text()
            make_download_dir()
            download_file(url_down)
            break

    print('The highest Hourly Dry Bulb Temperature is: ', highest_HourlyDryBulbTemperature())

    


if __name__ == "__main__":
    main()